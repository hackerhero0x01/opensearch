/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

/**
 * Remote upload pressure settings.
 */
public class RemoteUploadPressureSettings {

    public static final Setting<Boolean> REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED = Setting.boolSetting(
        "remote_store.segment_upload.pressure.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> MIN_SEQ_NO_LAG_LIMIT = Setting.longSetting(
        "remote_store.segment_upload.pressure.seq_no.lag.limit",
        2L,
        1L,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> MIN_BYTES_LAG_LIMIT = Setting.longSetting(
        "remote_store.segment_upload.pressure.bytes.lag.limit",
        10 * 1024 * 1024, // 10MB
        1024 * 1024, // 1MB
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MIN_TIME_LAG_LIMIT = Setting.timeSetting(
        "remote_store.segment_upload.pressure.time.lag.limit",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Long> MIN_INFLIGHT_BYTES_LAG_LIMIT_LIMIT = Setting.longSetting(
        "remote_store.segment_upload.pressure.bytes.lag.limit",
        10 * 1024 * 1024, // 10MB
        1024 * 1024, // 1MB
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MIN_CONSECUTIVE_FAILURES_LIMIT = Setting.intSetting(
        "remote_store.segment_upload.pressure.min_consecutive_failures.limit",
        10,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean remoteSegmentUploadPressureEnabled;

    private volatile long minSeqNoLagLimit;

    private volatile long minBytesLagLimit;

    private volatile TimeValue minTimeLagLimit;

    private volatile long minInflightBytesLagLimit;

    private volatile int minConsecutiveFailuresLimit;

    public RemoteUploadPressureSettings(ClusterService clusterService, Settings settings) {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();

        this.remoteSegmentUploadPressureEnabled = REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REMOTE_SEGMENT_UPLOAD_PRESSURE_ENABLED, this::setRemoteSegmentUploadPressureEnabled);

        this.minSeqNoLagLimit = MIN_SEQ_NO_LAG_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_SEQ_NO_LAG_LIMIT, this::setMinSeqNoLagLimit);

        this.minBytesLagLimit = MIN_BYTES_LAG_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_BYTES_LAG_LIMIT, this::setMinBytesLagLimit);

        this.minTimeLagLimit = MIN_TIME_LAG_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_TIME_LAG_LIMIT, this::setMinTimeLagLimit);

        this.minInflightBytesLagLimit = MIN_INFLIGHT_BYTES_LAG_LIMIT_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_INFLIGHT_BYTES_LAG_LIMIT_LIMIT, this::setMinInflightBytesLagLimit);

        this.minConsecutiveFailuresLimit = MIN_CONSECUTIVE_FAILURES_LIMIT.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MIN_CONSECUTIVE_FAILURES_LIMIT, this::setMinConsecutiveFailuresLimit);
    }

    public boolean isRemoteSegmentUploadPressureEnabled() {
        return remoteSegmentUploadPressureEnabled;
    }

    public void setRemoteSegmentUploadPressureEnabled(boolean remoteSegmentUploadPressureEnabled) {
        this.remoteSegmentUploadPressureEnabled = remoteSegmentUploadPressureEnabled;
    }

    public long getMinSeqNoLagLimit() {
        return minSeqNoLagLimit;
    }

    public void setMinSeqNoLagLimit(long minSeqNoLagLimit) {
        this.minSeqNoLagLimit = minSeqNoLagLimit;
    }

    public long getMinBytesLagLimit() {
        return minBytesLagLimit;
    }

    public void setMinBytesLagLimit(long minBytesLagLimit) {
        this.minBytesLagLimit = minBytesLagLimit;
    }

    public TimeValue getMinTimeLagLimit() {
        return minTimeLagLimit;
    }

    public void setMinTimeLagLimit(TimeValue minTimeLagLimit) {
        this.minTimeLagLimit = minTimeLagLimit;
    }

    public long getMinInflightBytesLagLimit() {
        return minInflightBytesLagLimit;
    }

    public void setMinInflightBytesLagLimit(long minInflightBytesLagLimit) {
        this.minInflightBytesLagLimit = minInflightBytesLagLimit;
    }

    public long getMinConsecutiveFailuresLimit() {
        return minConsecutiveFailuresLimit;
    }

    public void setMinConsecutiveFailuresLimit(int minConsecutiveFailuresLimit) {
        this.minConsecutiveFailuresLimit = minConsecutiveFailuresLimit;
    }
}
