/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.stats;

import org.opensearch.index.remote.RemoteRefreshSegmentTracker;
import org.opensearch.index.shard.ShardId;

import java.util.Map;

import static org.opensearch.test.OpenSearchTestCase.assertEquals;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;

/**
 * Helper utilities for Remote Store stats tests
 */
public class RemoteStoreStatsTestHelper {
    static RemoteRefreshSegmentTracker.Stats createPressureTrackerStats(ShardId shardId) {
        return new RemoteRefreshSegmentTracker.Stats(
            shardId,
            3,
            System.nanoTime() / 1_000_000L + randomIntBetween(10, 100),
            2,
            System.nanoTime() / 1_000_000L + randomIntBetween(10, 100),
            10,
            5,
            5,
            10,
            5,
            5,
            3,
            2,
            5,
            2,
            3,
            4,
            9
        );
    }

    static void compareStatsResponse(Map<String, Object> statsObject, RemoteRefreshSegmentTracker.Stats pressureTrackerStats) {
        assertEquals(statsObject.get(RemoteStoreStats.Fields.SHARD_ID), pressureTrackerStats.shardId.toString());
        assertEquals(statsObject.get(RemoteStoreStats.Fields.LOCAL_REFRESH_TIMESTAMP), (int) pressureTrackerStats.localRefreshTimeMs);
        assertEquals(statsObject.get(RemoteStoreStats.Fields.LOCAL_REFRESH_CUMULATIVE_COUNT), (int) pressureTrackerStats.localRefreshCount);
        assertEquals(statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_TIMESTAMP), (int) pressureTrackerStats.remoteRefreshTimeMs);
        assertEquals(
            statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_CUMULATIVE_COUNT),
            (int) pressureTrackerStats.remoteRefreshCount
        );
        assertEquals(statsObject.get(RemoteStoreStats.Fields.BYTES_LAG), (int) pressureTrackerStats.bytesLag);

        assertEquals(statsObject.get(RemoteStoreStats.Fields.REJECTION_COUNT), (int) pressureTrackerStats.rejectionCount);
        assertEquals(
            statsObject.get(RemoteStoreStats.Fields.CONSECUTIVE_FAILURE_COUNT),
            (int) pressureTrackerStats.consecutiveFailuresCount
        );

        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.Fields.STARTED),
            (int) pressureTrackerStats.uploadBytesStarted
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.Fields.SUCCEEDED),
            (int) pressureTrackerStats.uploadBytesSucceeded
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_UPLOADS_IN_BYTES)).get(RemoteStoreStats.Fields.FAILED),
            (int) pressureTrackerStats.uploadBytesFailed
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(RemoteStoreStats.Fields.MOVING_AVG),
            pressureTrackerStats.uploadBytesMovingAverage
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_SIZE_IN_BYTES)).get(RemoteStoreStats.Fields.LAST_SUCCESSFUL),
            (int) pressureTrackerStats.lastSuccessfulRemoteRefreshBytes
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.UPLOAD_LATENCY_IN_BYTES_PER_SEC)).get(RemoteStoreStats.Fields.MOVING_AVG),
            pressureTrackerStats.uploadBytesPerSecMovingAverage
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.Fields.STARTED),
            (int) pressureTrackerStats.totalUploadsStarted
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.Fields.SUCCEEDED),
            (int) pressureTrackerStats.totalUploadsSucceeded
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.TOTAL_REMOTE_REFRESH)).get(RemoteStoreStats.Fields.FAILED),
            (int) pressureTrackerStats.totalUploadsFailed
        );
        assertEquals(
            ((Map) statsObject.get(RemoteStoreStats.Fields.REMOTE_REFRESH_LATENCY_IN_NANOS)).get(RemoteStoreStats.Fields.MOVING_AVG),
            pressureTrackerStats.uploadTimeMovingAverage
        );
    }
}
