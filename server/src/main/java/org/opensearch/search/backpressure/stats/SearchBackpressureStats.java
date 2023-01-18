/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.search.backpressure.settings.SearchBackpressureMode;

import java.io.IOException;
import java.util.Objects;

/**
 * Stats related to search backpressure.
 */
public class SearchBackpressureStats implements ToXContentFragment, Writeable {
    private final SearchBackpressureTaskStats searchTaskStats;
    private final SearchBackpressureTaskStats searchShardTaskStats;
    private final SearchBackpressureMode mode;

    public SearchBackpressureStats(
        SearchBackpressureTaskStats searchTaskStats,
        SearchBackpressureTaskStats searchShardTaskStats,
        SearchBackpressureMode mode
    ) {
        this.searchTaskStats = searchTaskStats;
        this.searchShardTaskStats = searchShardTaskStats;
        this.mode = mode;
    }

    public SearchBackpressureStats(StreamInput in) throws IOException {
        searchShardTaskStats = new SearchBackpressureTaskStats(in);
        mode = SearchBackpressureMode.fromName(in.readString());
        searchTaskStats = in.readOptionalWriteable(SearchBackpressureTaskStats::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject("search_backpressure")
            .field("search_task", searchTaskStats)
            .field("search_shard_task", searchShardTaskStats)
            .field("mode", mode.getName())
            .endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        searchShardTaskStats.writeTo(out);
        out.writeString(mode.getName());
        // searchTaskStats.writeTo(out);
        out.writeOptionalWriteable(searchTaskStats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchBackpressureStats that = (SearchBackpressureStats) o;
        return mode == that.mode
            && Objects.equals(searchTaskStats, that.searchTaskStats)
            && Objects.equals(searchShardTaskStats, that.searchShardTaskStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchTaskStats, searchShardTaskStats, mode);
    }
}
