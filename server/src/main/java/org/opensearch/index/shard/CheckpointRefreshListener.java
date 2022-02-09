/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.indices.replication.checkpoint.TransportCheckpointPublisher;

import java.io.IOException;

/**
 * A {@link ReferenceManager.RefreshListener} that publishes a checkpoint to be consumed by replicas.
 */
public class CheckpointRefreshListener implements ReferenceManager.RefreshListener {

    protected static Logger logger = LogManager.getLogger(CheckpointRefreshListener.class);

    private final IndexShard shard;
    private final TransportCheckpointPublisher publisher;

    public CheckpointRefreshListener(IndexShard shard, TransportCheckpointPublisher publisher) {
        this.shard = shard;
        this.publisher = publisher;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Do nothing
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (shard.routingEntry().primary()) {
            publisher.publish(shard.getLatestReplicationCheckpoint());
        }
    }
}
