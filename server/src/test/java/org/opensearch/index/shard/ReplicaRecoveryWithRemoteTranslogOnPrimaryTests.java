/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.DocIdSeqNoAndSource;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.indices.replication.common.ReplicationType;

import java.util.List;

public class ReplicaRecoveryWithRemoteTranslogOnPrimaryTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, "true")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, "true")
        .build();

    public void testReplicaShardRecoveryUptoLastFlushedCommit() throws Exception {
        try (ReplicationGroup shards = createGroup(0, settings, new NRTReplicationEngineFactory())) {

            // Step1 - Start primary, index docs and flush
            shards.startPrimary();
            final IndexShard primary = shards.getPrimary();
            int numDocs = shards.indexDocs(randomIntBetween(10, 100));
            shards.flush();

            // Step 2 - Start replica for recovery to happen, check both has same number of docs
            final IndexShard replica1 = shards.addReplica();
            shards.startAll();
            assertEquals(getDocIdAndSeqNos(primary), getDocIdAndSeqNos(replica1));

            // Step 3 - Index more docs, run segment replication, check both have same number of docs
            int moreDocs = shards.indexDocs(randomIntBetween(10, 100));
            primary.refresh("test");
            replicateSegments(primary, shards.getReplicas());
            assertEquals(getDocIdAndSeqNos(primary), getDocIdAndSeqNos(replica1));

            // Step 4 - Check both shard has expected number of doc count
            assertDocCount(primary, numDocs + moreDocs);
            assertDocCount(replica1, numDocs + moreDocs);

            // Step 5 - Start new replica, recovery happens, and check that new replica has docs upto last flush
            final IndexShard replica2 = shards.addReplica();
            shards.startAll();
            assertDocCount(replica2, numDocs);

            // Step 6 - Segment replication, check all shards have same number of docs
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs + moreDocs);
        }
    }

    public void testNoTranslogHistoryTransferred() throws Exception {
        try (ReplicationGroup shards = createGroup(0, settings, new NRTReplicationEngineFactory())) {

            // Step1 - Start primary, index docs, flush, index more docs, check translog in primary as expected
            shards.startPrimary();
            final IndexShard primary = shards.getPrimary();
            int numDocs = shards.indexDocs(randomIntBetween(10, 100));
            shards.flush();
            List<DocIdSeqNoAndSource> docIdAndSeqNosAfterFlush = getDocIdAndSeqNos(primary);
            int moreDocs = shards.indexDocs(randomIntBetween(20, 100));
            assertEquals(moreDocs, getTranslog(primary).totalOperations());

            // Step 2 - Start replica, recovery happens, check docs recovered till last flush
            final IndexShard replica = shards.addReplica();
            shards.startAll();
            assertEquals(docIdAndSeqNosAfterFlush, getDocIdAndSeqNos(replica));
            assertDocCount(replica, numDocs);
            assertEquals(NRTReplicationEngine.class, replica.getEngine().getClass());

            // Step 3 - Check replica's translog has no operations
            assertEquals(WriteOnlyTranslogManager.class, replica.getEngine().translogManager().getClass());
            WriteOnlyTranslogManager replicaTranslogManager = (WriteOnlyTranslogManager) replica.getEngine().translogManager();
            assertEquals(0, replicaTranslogManager.getTranslog().totalOperations());

            // Adding this for close to succeed
            shards.flush();
            replicateSegments(primary, shards.getReplicas());
            shards.assertAllEqual(numDocs + moreDocs);
        }
    }
}
