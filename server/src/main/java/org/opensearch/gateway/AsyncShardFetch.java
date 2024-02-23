/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gateway;

import org.apache.logging.log4j.Logger;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.ShardAttributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * Allows to asynchronously fetch shard related data from other nodes for allocation, without blocking
 * the cluster update thread.
 * <p>
 * The async fetch logic maintains a map of which nodes are being fetched from in an async manner,
 * and once the results are back, it makes sure to schedule a reroute to make sure those results will
 * be taken into account.
 *
 * It comes in two modes, to single fetch a shard or fetch a batch of shards.
 * @opensearch.internal
 */
public abstract class AsyncShardFetch<T extends BaseNodeResponse> implements Releasable {

    /**
     * An action that lists the relevant shard data that needs to be fetched.
     */
    public interface Lister<NodesResponse extends BaseNodesResponse<NodeResponse>, NodeResponse extends BaseNodeResponse> {
        void list(Map<ShardId, ShardAttributes> shardAttributesMap, DiscoveryNode[] nodes, ActionListener<NodesResponse> listener);

    }

    protected final Logger logger;
    protected final String type;
    protected final Map<ShardId, ShardAttributes> shardAttributesMap;
    private final Lister<BaseNodesResponse<T>, T> action;
    private final BaseShardCache<T> cache;
    private final AtomicLong round = new AtomicLong();
    private boolean closed;
    private final String reroutingKey;
    private final Map<ShardId, Set<String>> shardToIgnoreNodes = new HashMap<>();

    private final boolean enableBatchMode;

    @SuppressWarnings("unchecked")
    protected AsyncShardFetch(
        Logger logger,
        String type,
        ShardId shardId,
        String customDataPath,
        Lister<? extends BaseNodesResponse<T>, T> action
    ) {
        this.logger = logger;
        this.type = type;
        shardAttributesMap = new HashMap<>();
        shardAttributesMap.put(shardId, new ShardAttributes(shardId, customDataPath));
        this.action = (Lister<BaseNodesResponse<T>, T>) action;
        this.reroutingKey = "ShardId=[" + shardId.toString() + "]";
        enableBatchMode = false;
        cache = new ShardCache<>(logger, reroutingKey, type);
    }

    /**
     * Added to fetch a batch of shards from nodes
     *
     * @param logger Logger
     * @param type type of action
     * @param shardAttributesMap Map of {@link ShardId} to {@link ShardAttributes} to perform fetching on them a
     * @param action Transport Action
     * @param batchId For the given ShardAttributesMap, we expect them to tie with a single batch id for logging and later identification
     */
    @SuppressWarnings("unchecked")
    protected AsyncShardFetch(
        Logger logger,
        String type,
        Map<ShardId, ShardAttributes> shardAttributesMap,
        Lister<? extends BaseNodesResponse<T>, T> action,
        String batchId
    ) {
        this.logger = logger;
        this.type = type;
        this.shardAttributesMap = shardAttributesMap;
        this.action = (Lister<BaseNodesResponse<T>, T>) action;
        this.reroutingKey = "BatchID=[" + batchId + "]";
        enableBatchMode = true;
        cache = new ShardCache<>(logger, reroutingKey, type);
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    /**
     * Fetches the data for the relevant shard. If there any ongoing async fetches going on, or new ones have
     * been initiated by this call, the result will have no data.
     * <p>
     * The ignoreNodes are nodes that are supposed to be ignored for this round, since fetching is async, we need
     * to keep them around and make sure we add them back when all the responses are fetched and returned.
     */
    public synchronized FetchResult<T> fetchData(DiscoveryNodes nodes, Map<ShardId, Set<String>> ignoreNodes) {
        if (closed) {
            throw new IllegalStateException(reroutingKey + ": can't fetch data on closed async fetch");
        }

        if (enableBatchMode == false) {
            // we will do assertions here on ignoreNodes
            if (ignoreNodes.size() > 1) {
                throw new IllegalStateException(
                    "Fetching Shard Data, " + reroutingKey + "Can only have atmost one shard" + "for non-batch mode"
                );
            }
            if (ignoreNodes.size() == 1) {
                if (shardAttributesMap.containsKey(ignoreNodes.keySet().iterator().next()) == false) {
                    throw new IllegalStateException("Shard Id must be same as initialized in AsyncShardFetch. Expecting = " + reroutingKey);
                }
            }
        }

        // add the nodes to ignore to the list of nodes to ignore for each shard
        for (Map.Entry<ShardId, Set<String>> ignoreNodesEntry : ignoreNodes.entrySet()) {
            Set<String> ignoreNodesSet = shardToIgnoreNodes.getOrDefault(ignoreNodesEntry.getKey(), new HashSet<>());
            ignoreNodesSet.addAll(ignoreNodesEntry.getValue());
            shardToIgnoreNodes.put(ignoreNodesEntry.getKey(), ignoreNodesSet);
        }

        cache.fillShardCacheWithDataNodes(nodes);
        List<String> nodeIds = cache.findNodesToFetch();
        if (nodeIds.isEmpty() == false) {
            // mark all node as fetching and go ahead and async fetch them
            // use a unique round id to detect stale responses in processAsyncFetch
            final long fetchingRound = round.incrementAndGet();
            cache.markAsFetching(nodeIds, fetchingRound);
            DiscoveryNode[] discoNodesToFetch = nodeIds.stream().map(nodes::get).toArray(DiscoveryNode[]::new);
            asyncFetch(discoNodesToFetch, fetchingRound);
        }

        // if we are still fetching, return null to indicate it
        if (cache.hasAnyNodeFetching()) {
            return new FetchResult<>(null, emptyMap());
        } else {
            // nothing to fetch, yay, build the return value
            Set<String> failedNodes = new HashSet<>();
            Map<DiscoveryNode, T> fetchData = cache.populateCache(nodes, failedNodes);

            Map<ShardId, Set<String>> allIgnoreNodesMap = unmodifiableMap(new HashMap<>(shardToIgnoreNodes));
            // clear the nodes to ignore, we had a successful run in fetching everything we can
            // we need to try them if another full run is needed
            shardToIgnoreNodes.clear();
            // if at least one node failed, make sure to have a protective reroute
            // here, just case this round won't find anything, and we need to retry fetching data

            if (failedNodes.isEmpty() == false
                || allIgnoreNodesMap.values().stream().anyMatch(ignoreNodeSet -> ignoreNodeSet.isEmpty() == false)) {
                reroute(
                    reroutingKey,
                    "nodes failed ["
                        + failedNodes.size()
                        + "], ignored ["
                        + allIgnoreNodesMap.values().stream().mapToInt(Set::size).sum()
                        + "]"
                );
            }

            return new FetchResult<>(fetchData, allIgnoreNodesMap);
        }
    }

    /**
     * Called by the response handler of the async action to fetch data. Verifies that its still working
     * on the same cache generation, otherwise the results are discarded. It then goes and fills the relevant data for
     * the shard (response + failures), issuing a reroute at the end of it to make sure there will be another round
     * of allocations taking this new data into account.
     */
    protected synchronized void processAsyncFetch(List<T> responses, List<FailedNodeException> failures, long fetchingRound) {
        if (closed) {
            // we are closed, no need to process this async fetch at all
            logger.trace("{} ignoring fetched [{}] results, already closed", reroutingKey, type);
            return;
        }
        logger.trace("{} processing fetched [{}] results", reroutingKey, type);

        if (responses != null) {
            cache.processResponses(responses, fetchingRound);
        }
        if (failures != null) {
            cache.processFailures(failures, fetchingRound);
        }
        reroute(reroutingKey, "post_response");
    }

    public int getNumberOfInFlightFetches() {
        return cache.getInflightFetches();
    }

    /**
     * Implement this in order to scheduled another round that causes a call to fetch data.
     */
    protected abstract void reroute(String reroutingKey, String reason);

    /**
     * Clear cache for node, ensuring next fetch will fetch a fresh copy.
     */
    synchronized void clearCacheForNode(String nodeId) {
        cache.remove(nodeId);
    }

    /**
     * Async fetches data for the provided shard with the set of nodes that need to be fetched from.
     */
    // visible for testing
    void asyncFetch(final DiscoveryNode[] nodes, long fetchingRound) {
        logger.trace("{} fetching [{}] from {}", reroutingKey, type, nodes);
        action.list(shardAttributesMap, nodes, new ActionListener<BaseNodesResponse<T>>() {
            @Override
            public void onResponse(BaseNodesResponse<T> response) {
                processAsyncFetch(response.getNodes(), response.failures(), fetchingRound);
            }

            @Override
            public void onFailure(Exception e) {
                List<FailedNodeException> failures = new ArrayList<>(nodes.length);
                for (final DiscoveryNode node : nodes) {
                    failures.add(new FailedNodeException(node.getId(), "total failure in fetching", e));
                }
                processAsyncFetch(null, failures, fetchingRound);
            }
        });
    }

    /**
     * The result of a fetch operation. Make sure to first check {@link #hasData()} before
     * fetching the actual data.
     */
    public static class FetchResult<T extends BaseNodeResponse> {

        private final Map<DiscoveryNode, T> data;
        private final Map<ShardId, Set<String>> ignoredShardToNodes;

        public FetchResult(Map<DiscoveryNode, T> data, Map<ShardId, Set<String>> ignoreNodes) {
            this.data = data;
            this.ignoredShardToNodes = ignoreNodes;
        }

        /**
         * Does the result actually contain data? If not, then there are on going fetch
         * operations happening, and it should wait for it.
         */
        public boolean hasData() {
            return data != null;
        }

        /**
         * Returns the actual data, note, make sure to check {@link #hasData()} first and
         * only use this when there is an actual data.
         */
        public Map<DiscoveryNode, T> getData() {
            assert data != null : "getData should only be called if there is data to be fetched, please check hasData first";
            return this.data;
        }

        /**
         * Process any changes needed to the allocation based on this fetch result.
         */
        public void processAllocation(RoutingAllocation allocation) {
            for (Map.Entry<ShardId, Set<String>> entry : ignoredShardToNodes.entrySet()) {
                ShardId shardId = entry.getKey();
                Set<String> ignoreNodes = entry.getValue();
                if (ignoreNodes.isEmpty() == false) {
                    ignoreNodes.forEach(nodeId -> allocation.addIgnoreShardForNode(shardId, nodeId));
                }
            }

        }
    }
}
