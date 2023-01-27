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

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.FailOpenWeightedRoutingStats;
import org.opensearch.cluster.service.ClusterManagerThrottlingStats;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.cluster.coordination.PendingClusterStateStats;
import org.opensearch.cluster.coordination.PublishClusterStateStats;
import org.opensearch.http.HttpStats;
import org.opensearch.indices.breaker.AllCircuitBreakerStats;
import org.opensearch.indices.breaker.CircuitBreakerStats;
import org.opensearch.ingest.IngestStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.node.AdaptiveSelectionStats;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.script.ScriptCacheStats;
import org.opensearch.script.ScriptStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.TransportStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class NodeStatsTests extends OpenSearchTestCase {
    public void testSerialization() throws IOException {
        NodeStats nodeStats = createNodeStats();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserializedNodeStats = new NodeStats(in);
                assertEquals(nodeStats.getNode(), deserializedNodeStats.getNode());
                assertEquals(nodeStats.getTimestamp(), deserializedNodeStats.getTimestamp());
                if (nodeStats.getOs() == null) {
                    assertNull(deserializedNodeStats.getOs());
                } else {
                    assertEquals(nodeStats.getOs().getTimestamp(), deserializedNodeStats.getOs().getTimestamp());
                    assertEquals(nodeStats.getOs().getSwap().getFree(), deserializedNodeStats.getOs().getSwap().getFree());
                    assertEquals(nodeStats.getOs().getSwap().getTotal(), deserializedNodeStats.getOs().getSwap().getTotal());
                    assertEquals(nodeStats.getOs().getSwap().getUsed(), deserializedNodeStats.getOs().getSwap().getUsed());
                    assertEquals(nodeStats.getOs().getMem().getFree(), deserializedNodeStats.getOs().getMem().getFree());
                    assertEquals(nodeStats.getOs().getMem().getTotal(), deserializedNodeStats.getOs().getMem().getTotal());
                    assertEquals(nodeStats.getOs().getMem().getUsed(), deserializedNodeStats.getOs().getMem().getUsed());
                    assertEquals(nodeStats.getOs().getMem().getFreePercent(), deserializedNodeStats.getOs().getMem().getFreePercent());
                    assertEquals(nodeStats.getOs().getMem().getUsedPercent(), deserializedNodeStats.getOs().getMem().getUsedPercent());
                    assertEquals(nodeStats.getOs().getCpu().getPercent(), deserializedNodeStats.getOs().getCpu().getPercent());
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuAcctControlGroup(),
                        deserializedNodeStats.getOs().getCgroup().getCpuAcctControlGroup()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuAcctUsageNanos(),
                        deserializedNodeStats.getOs().getCgroup().getCpuAcctUsageNanos()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuControlGroup(),
                        deserializedNodeStats.getOs().getCgroup().getCpuControlGroup()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuCfsPeriodMicros(),
                        deserializedNodeStats.getOs().getCgroup().getCpuCfsPeriodMicros()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuCfsQuotaMicros(),
                        deserializedNodeStats.getOs().getCgroup().getCpuCfsQuotaMicros()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getNumberOfElapsedPeriods(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getNumberOfElapsedPeriods()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getNumberOfTimesThrottled(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getNumberOfTimesThrottled()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getTimeThrottledNanos(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getTimeThrottledNanos()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getMemoryLimitInBytes(),
                        deserializedNodeStats.getOs().getCgroup().getMemoryLimitInBytes()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getMemoryUsageInBytes(),
                        deserializedNodeStats.getOs().getCgroup().getMemoryUsageInBytes()
                    );
                    assertArrayEquals(
                        nodeStats.getOs().getCpu().getLoadAverage(),
                        deserializedNodeStats.getOs().getCpu().getLoadAverage(),
                        0
                    );
                }
                if (nodeStats.getProcess() == null) {
                    assertNull(deserializedNodeStats.getProcess());
                } else {
                    assertEquals(nodeStats.getProcess().getTimestamp(), deserializedNodeStats.getProcess().getTimestamp());
                    assertEquals(nodeStats.getProcess().getCpu().getTotal(), deserializedNodeStats.getProcess().getCpu().getTotal());
                    assertEquals(nodeStats.getProcess().getCpu().getPercent(), deserializedNodeStats.getProcess().getCpu().getPercent());
                    assertEquals(
                        nodeStats.getProcess().getMem().getTotalVirtual(),
                        deserializedNodeStats.getProcess().getMem().getTotalVirtual()
                    );
                    assertEquals(
                        nodeStats.getProcess().getMaxFileDescriptors(),
                        deserializedNodeStats.getProcess().getMaxFileDescriptors()
                    );
                    assertEquals(
                        nodeStats.getProcess().getOpenFileDescriptors(),
                        deserializedNodeStats.getProcess().getOpenFileDescriptors()
                    );
                }
                JvmStats jvm = nodeStats.getJvm();
                JvmStats deserializedJvm = deserializedNodeStats.getJvm();
                if (jvm == null) {
                    assertNull(deserializedJvm);
                } else {
                    JvmStats.Mem mem = jvm.getMem();
                    JvmStats.Mem deserializedMem = deserializedJvm.getMem();
                    assertEquals(jvm.getTimestamp(), deserializedJvm.getTimestamp());
                    assertEquals(mem.getHeapUsedPercent(), deserializedMem.getHeapUsedPercent());
                    assertEquals(mem.getHeapUsed(), deserializedMem.getHeapUsed());
                    assertEquals(mem.getHeapCommitted(), deserializedMem.getHeapCommitted());
                    assertEquals(mem.getNonHeapCommitted(), deserializedMem.getNonHeapCommitted());
                    assertEquals(mem.getNonHeapUsed(), deserializedMem.getNonHeapUsed());
                    assertEquals(mem.getHeapMax(), deserializedMem.getHeapMax());

                    final Map<String, JvmStats.MemoryPool> pools = StreamSupport.stream(mem.spliterator(), false)
                        .collect(Collectors.toMap(JvmStats.MemoryPool::getName, Function.identity()));

                    final Map<String, JvmStats.MemoryPool> deserializedPools = StreamSupport.stream(deserializedMem.spliterator(), false)
                        .collect(Collectors.toMap(JvmStats.MemoryPool::getName, Function.identity()));

                    final int poolsCount = (int) StreamSupport.stream(nodeStats.getJvm().getMem().spliterator(), false).count();
                    assertThat(pools.keySet(), hasSize(poolsCount));
                    assertThat(deserializedPools.keySet(), hasSize(poolsCount));

                    for (final Map.Entry<String, JvmStats.MemoryPool> entry : pools.entrySet()) {
                        assertThat(deserializedPools.containsKey(entry.getKey()), is(true));
                        assertEquals(entry.getValue().getName(), deserializedPools.get(entry.getKey()).getName());
                        assertEquals(entry.getValue().getMax(), deserializedPools.get(entry.getKey()).getMax());
                        assertEquals(entry.getValue().getPeakMax(), deserializedPools.get(entry.getKey()).getPeakMax());
                        assertEquals(entry.getValue().getPeakUsed(), deserializedPools.get(entry.getKey()).getPeakUsed());
                        assertEquals(entry.getValue().getUsed(), deserializedPools.get(entry.getKey()).getUsed());

                        assertEquals(
                            entry.getValue().getLastGcStats().getUsed(),
                            deserializedPools.get(entry.getKey()).getLastGcStats().getUsed()
                        );
                        assertEquals(
                            entry.getValue().getLastGcStats().getMax(),
                            deserializedPools.get(entry.getKey()).getLastGcStats().getMax()
                        );
                        assertEquals(
                            entry.getValue().getLastGcStats().getUsagePercent(),
                            deserializedPools.get(entry.getKey()).getLastGcStats().getUsagePercent()
                        );
                    }

                    JvmStats.Classes classes = jvm.getClasses();
                    assertEquals(classes.getLoadedClassCount(), deserializedJvm.getClasses().getLoadedClassCount());
                    assertEquals(classes.getTotalLoadedClassCount(), deserializedJvm.getClasses().getTotalLoadedClassCount());
                    assertEquals(classes.getUnloadedClassCount(), deserializedJvm.getClasses().getUnloadedClassCount());
                    assertEquals(jvm.getGc().getCollectors().length, deserializedJvm.getGc().getCollectors().length);
                    for (int i = 0; i < jvm.getGc().getCollectors().length; i++) {
                        JvmStats.GarbageCollector garbageCollector = jvm.getGc().getCollectors()[i];
                        JvmStats.GarbageCollector deserializedGarbageCollector = deserializedJvm.getGc().getCollectors()[i];
                        assertEquals(garbageCollector.getName(), deserializedGarbageCollector.getName());
                        assertEquals(garbageCollector.getCollectionCount(), deserializedGarbageCollector.getCollectionCount());
                        assertEquals(garbageCollector.getCollectionTime(), deserializedGarbageCollector.getCollectionTime());
                    }
                    assertEquals(jvm.getThreads().getCount(), deserializedJvm.getThreads().getCount());
                    assertEquals(jvm.getThreads().getPeakCount(), deserializedJvm.getThreads().getPeakCount());
                    assertEquals(jvm.getUptime(), deserializedJvm.getUptime());
                    if (jvm.getBufferPools() == null) {
                        assertNull(deserializedJvm.getBufferPools());
                    } else {
                        assertEquals(jvm.getBufferPools().size(), deserializedJvm.getBufferPools().size());
                        for (int i = 0; i < jvm.getBufferPools().size(); i++) {
                            JvmStats.BufferPool bufferPool = jvm.getBufferPools().get(i);
                            JvmStats.BufferPool deserializedBufferPool = deserializedJvm.getBufferPools().get(i);
                            assertEquals(bufferPool.getName(), deserializedBufferPool.getName());
                            assertEquals(bufferPool.getCount(), deserializedBufferPool.getCount());
                            assertEquals(bufferPool.getTotalCapacity(), deserializedBufferPool.getTotalCapacity());
                            assertEquals(bufferPool.getUsed(), deserializedBufferPool.getUsed());
                        }
                    }
                }
                if (nodeStats.getThreadPool() == null) {
                    assertNull(deserializedNodeStats.getThreadPool());
                } else {
                    Iterator<ThreadPoolStats.Stats> threadPoolIterator = nodeStats.getThreadPool().iterator();
                    Iterator<ThreadPoolStats.Stats> deserializedThreadPoolIterator = deserializedNodeStats.getThreadPool().iterator();
                    while (threadPoolIterator.hasNext()) {
                        ThreadPoolStats.Stats stats = threadPoolIterator.next();
                        ThreadPoolStats.Stats deserializedStats = deserializedThreadPoolIterator.next();
                        assertEquals(stats.getName(), deserializedStats.getName());
                        assertEquals(stats.getThreads(), deserializedStats.getThreads());
                        assertEquals(stats.getActive(), deserializedStats.getActive());
                        assertEquals(stats.getLargest(), deserializedStats.getLargest());
                        assertEquals(stats.getCompleted(), deserializedStats.getCompleted());
                        assertEquals(stats.getQueue(), deserializedStats.getQueue());
                        assertEquals(stats.getRejected(), deserializedStats.getRejected());
                    }
                }
                FsInfo fs = nodeStats.getFs();
                FsInfo deserializedFs = deserializedNodeStats.getFs();
                if (fs == null) {
                    assertNull(deserializedFs);
                } else {
                    assertEquals(fs.getTimestamp(), deserializedFs.getTimestamp());
                    assertEquals(fs.getTotal().getAvailable(), deserializedFs.getTotal().getAvailable());
                    assertEquals(fs.getTotal().getTotal(), deserializedFs.getTotal().getTotal());
                    assertEquals(fs.getTotal().getFree(), deserializedFs.getTotal().getFree());
                    assertEquals(fs.getTotal().getMount(), deserializedFs.getTotal().getMount());
                    assertEquals(fs.getTotal().getPath(), deserializedFs.getTotal().getPath());
                    assertEquals(fs.getTotal().getType(), deserializedFs.getTotal().getType());
                    FsInfo.IoStats ioStats = fs.getIoStats();
                    FsInfo.IoStats deserializedIoStats = deserializedFs.getIoStats();
                    assertEquals(ioStats.getTotalOperations(), deserializedIoStats.getTotalOperations());
                    assertEquals(ioStats.getTotalReadKilobytes(), deserializedIoStats.getTotalReadKilobytes());
                    assertEquals(ioStats.getTotalReadOperations(), deserializedIoStats.getTotalReadOperations());
                    assertEquals(ioStats.getTotalWriteKilobytes(), deserializedIoStats.getTotalWriteKilobytes());
                    assertEquals(ioStats.getTotalWriteOperations(), deserializedIoStats.getTotalWriteOperations());
                    assertEquals(ioStats.getDevicesStats().length, deserializedIoStats.getDevicesStats().length);
                    for (int i = 0; i < ioStats.getDevicesStats().length; i++) {
                        FsInfo.DeviceStats deviceStats = ioStats.getDevicesStats()[i];
                        FsInfo.DeviceStats deserializedDeviceStats = deserializedIoStats.getDevicesStats()[i];
                        assertEquals(deviceStats.operations(), deserializedDeviceStats.operations());
                        assertEquals(deviceStats.readKilobytes(), deserializedDeviceStats.readKilobytes());
                        assertEquals(deviceStats.readOperations(), deserializedDeviceStats.readOperations());
                        assertEquals(deviceStats.writeKilobytes(), deserializedDeviceStats.writeKilobytes());
                        assertEquals(deviceStats.writeOperations(), deserializedDeviceStats.writeOperations());
                    }
                }
                if (nodeStats.getTransport() == null) {
                    assertNull(deserializedNodeStats.getTransport());
                } else {
                    assertEquals(nodeStats.getTransport().getRxCount(), deserializedNodeStats.getTransport().getRxCount());
                    assertEquals(nodeStats.getTransport().getRxSize(), deserializedNodeStats.getTransport().getRxSize());
                    assertEquals(nodeStats.getTransport().getServerOpen(), deserializedNodeStats.getTransport().getServerOpen());
                    assertEquals(nodeStats.getTransport().getTxCount(), deserializedNodeStats.getTransport().getTxCount());
                    assertEquals(nodeStats.getTransport().getTxSize(), deserializedNodeStats.getTransport().getTxSize());
                }
                if (nodeStats.getHttp() == null) {
                    assertNull(deserializedNodeStats.getHttp());
                } else {
                    assertEquals(nodeStats.getHttp().getServerOpen(), deserializedNodeStats.getHttp().getServerOpen());
                    assertEquals(nodeStats.getHttp().getTotalOpen(), deserializedNodeStats.getHttp().getTotalOpen());
                }
                if (nodeStats.getBreaker() == null) {
                    assertNull(deserializedNodeStats.getBreaker());
                } else {
                    assertEquals(nodeStats.getBreaker().getAllStats().length, deserializedNodeStats.getBreaker().getAllStats().length);
                    for (int i = 0; i < nodeStats.getBreaker().getAllStats().length; i++) {
                        CircuitBreakerStats circuitBreakerStats = nodeStats.getBreaker().getAllStats()[i];
                        CircuitBreakerStats deserializedCircuitBreakerStats = deserializedNodeStats.getBreaker().getAllStats()[i];
                        assertEquals(circuitBreakerStats.getEstimated(), deserializedCircuitBreakerStats.getEstimated());
                        assertEquals(circuitBreakerStats.getLimit(), deserializedCircuitBreakerStats.getLimit());
                        assertEquals(circuitBreakerStats.getName(), deserializedCircuitBreakerStats.getName());
                        assertEquals(circuitBreakerStats.getOverhead(), deserializedCircuitBreakerStats.getOverhead(), 0);
                        assertEquals(circuitBreakerStats.getTrippedCount(), deserializedCircuitBreakerStats.getTrippedCount(), 0);
                    }
                }
                ScriptStats scriptStats = nodeStats.getScriptStats();
                if (scriptStats == null) {
                    assertNull(deserializedNodeStats.getScriptStats());
                } else {
                    assertEquals(scriptStats.getCacheEvictions(), deserializedNodeStats.getScriptStats().getCacheEvictions());
                    assertEquals(scriptStats.getCompilations(), deserializedNodeStats.getScriptStats().getCompilations());
                }
                DiscoveryStats discoveryStats = nodeStats.getDiscoveryStats();
                DiscoveryStats deserializedDiscoveryStats = deserializedNodeStats.getDiscoveryStats();
                if (discoveryStats == null) {
                    assertNull(deserializedDiscoveryStats);
                } else {
                    PendingClusterStateStats queueStats = discoveryStats.getQueueStats();
                    if (queueStats == null) {
                        assertNull(deserializedDiscoveryStats.getQueueStats());
                    } else {
                        assertEquals(queueStats.getCommitted(), deserializedDiscoveryStats.getQueueStats().getCommitted());
                        assertEquals(queueStats.getTotal(), deserializedDiscoveryStats.getQueueStats().getTotal());
                        assertEquals(queueStats.getPending(), deserializedDiscoveryStats.getQueueStats().getPending());
                    }
                }
                IngestStats ingestStats = nodeStats.getIngestStats();
                IngestStats deserializedIngestStats = deserializedNodeStats.getIngestStats();
                if (ingestStats == null) {
                    assertNull(deserializedIngestStats);
                } else {
                    IngestStats.Stats totalStats = ingestStats.getTotalStats();
                    assertEquals(totalStats.getIngestCount(), deserializedIngestStats.getTotalStats().getIngestCount());
                    assertEquals(totalStats.getIngestCurrent(), deserializedIngestStats.getTotalStats().getIngestCurrent());
                    assertEquals(totalStats.getIngestFailedCount(), deserializedIngestStats.getTotalStats().getIngestFailedCount());
                    assertEquals(totalStats.getIngestTimeInMillis(), deserializedIngestStats.getTotalStats().getIngestTimeInMillis());
                    assertEquals(ingestStats.getPipelineStats().size(), deserializedIngestStats.getPipelineStats().size());
                    for (IngestStats.PipelineStat pipelineStat : ingestStats.getPipelineStats()) {
                        String pipelineId = pipelineStat.getPipelineId();
                        IngestStats.Stats deserializedPipelineStats = getPipelineStats(
                            deserializedIngestStats.getPipelineStats(),
                            pipelineId
                        );
                        assertEquals(pipelineStat.getStats().getIngestFailedCount(), deserializedPipelineStats.getIngestFailedCount());
                        assertEquals(pipelineStat.getStats().getIngestTimeInMillis(), deserializedPipelineStats.getIngestTimeInMillis());
                        assertEquals(pipelineStat.getStats().getIngestCurrent(), deserializedPipelineStats.getIngestCurrent());
                        assertEquals(pipelineStat.getStats().getIngestCount(), deserializedPipelineStats.getIngestCount());
                        List<IngestStats.ProcessorStat> processorStats = ingestStats.getProcessorStats().get(pipelineId);
                        // intentionally validating identical order
                        Iterator<IngestStats.ProcessorStat> it = deserializedIngestStats.getProcessorStats().get(pipelineId).iterator();
                        for (IngestStats.ProcessorStat processorStat : processorStats) {
                            IngestStats.ProcessorStat deserializedProcessorStat = it.next();
                            assertEquals(
                                processorStat.getStats().getIngestFailedCount(),
                                deserializedProcessorStat.getStats().getIngestFailedCount()
                            );
                            assertEquals(
                                processorStat.getStats().getIngestTimeInMillis(),
                                deserializedProcessorStat.getStats().getIngestTimeInMillis()
                            );
                            assertEquals(
                                processorStat.getStats().getIngestCurrent(),
                                deserializedProcessorStat.getStats().getIngestCurrent()
                            );
                            assertEquals(processorStat.getStats().getIngestCount(), deserializedProcessorStat.getStats().getIngestCount());
                        }
                        assertFalse(it.hasNext());
                    }
                }
                AdaptiveSelectionStats adaptiveStats = nodeStats.getAdaptiveSelectionStats();
                AdaptiveSelectionStats deserializedAdaptiveStats = deserializedNodeStats.getAdaptiveSelectionStats();
                if (adaptiveStats == null) {
                    assertNull(deserializedAdaptiveStats);
                } else {
                    assertEquals(adaptiveStats.getOutgoingConnections(), deserializedAdaptiveStats.getOutgoingConnections());
                    assertEquals(adaptiveStats.getRanks(), deserializedAdaptiveStats.getRanks());
                    adaptiveStats.getComputedStats().forEach((k, v) -> {
                        ResponseCollectorService.ComputedNodeStats aStats = adaptiveStats.getComputedStats().get(k);
                        ResponseCollectorService.ComputedNodeStats bStats = deserializedAdaptiveStats.getComputedStats().get(k);
                        assertEquals(aStats.nodeId, bStats.nodeId);
                        assertEquals(aStats.queueSize, bStats.queueSize, 0.01);
                        assertEquals(aStats.serviceTime, bStats.serviceTime, 0.01);
                        assertEquals(aStats.responseTime, bStats.responseTime, 0.01);
                    });
                }
                ScriptCacheStats scriptCacheStats = nodeStats.getScriptCacheStats();
                ScriptCacheStats deserializedScriptCacheStats = deserializedNodeStats.getScriptCacheStats();
                if (scriptCacheStats == null) {
                    assertNull(deserializedScriptCacheStats);
                } else if (deserializedScriptCacheStats.getContextStats() != null) {
                    Map<String, ScriptStats> deserialized = deserializedScriptCacheStats.getContextStats();
                    long evictions = 0;
                    long limited = 0;
                    long compilations = 0;
                    Map<String, ScriptStats> stats = scriptCacheStats.getContextStats();
                    for (String context : stats.keySet()) {
                        ScriptStats deserStats = deserialized.get(context);
                        ScriptStats generatedStats = stats.get(context);

                        evictions += generatedStats.getCacheEvictions();
                        assertEquals(generatedStats.getCacheEvictions(), deserStats.getCacheEvictions());

                        limited += generatedStats.getCompilationLimitTriggered();
                        assertEquals(generatedStats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());

                        compilations += generatedStats.getCompilations();
                        assertEquals(generatedStats.getCompilations(), deserStats.getCompilations());
                    }
                    ScriptStats sum = deserializedScriptCacheStats.sum();
                    assertEquals(evictions, sum.getCacheEvictions());
                    assertEquals(limited, sum.getCompilationLimitTriggered());
                    assertEquals(compilations, sum.getCompilations());
                }
                ClusterManagerThrottlingStats clusterManagerThrottlingStats = nodeStats.getClusterManagerThrottlingStats();
                ClusterManagerThrottlingStats deserializedClusterManagerThrottlingStats = deserializedNodeStats
                    .getClusterManagerThrottlingStats();
                if (clusterManagerThrottlingStats == null) {
                    assertNull(deserializedClusterManagerThrottlingStats);
                } else {
                    assertEquals(
                        clusterManagerThrottlingStats.getTotalThrottledTaskCount(),
                        deserializedClusterManagerThrottlingStats.getTotalThrottledTaskCount()
                    );
                    assertEquals(
                        clusterManagerThrottlingStats.getThrottlingCount("test-task"),
                        deserializedClusterManagerThrottlingStats.getThrottlingCount("test-task")
                    );
                }
            }
        }
    }

    public static NodeStats createNodeStats() {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        OsStats osStats = null;
        if (frequently()) {
            double loadAverages[] = new double[3];
            for (int i = 0; i < 3; i++) {
                loadAverages[i] = randomBoolean() ? randomDouble() : -1;
            }
            long memTotal = randomNonNegativeLong();
            long swapTotal = randomNonNegativeLong();
            osStats = new OsStats(
                System.currentTimeMillis(),
                new OsStats.Cpu(randomShort(), loadAverages),
                new OsStats.Mem(memTotal, randomLongBetween(0, memTotal)),
                new OsStats.Swap(swapTotal, randomLongBetween(0, swapTotal)),
                new OsStats.Cgroup(
                    randomAlphaOfLength(8),
                    randomNonNegativeLong(),
                    randomAlphaOfLength(8),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    new OsStats.Cgroup.CpuStat(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
                    randomAlphaOfLength(8),
                    Long.toString(randomNonNegativeLong()),
                    Long.toString(randomNonNegativeLong())
                )
            );
        }
        ProcessStats processStats = frequently()
            ? new ProcessStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new ProcessStats.Cpu(randomShort(), randomNonNegativeLong()),
                new ProcessStats.Mem(randomNonNegativeLong())
            )
            : null;
        JvmStats jvmStats = null;
        if (frequently()) {
            int numMemoryPools = randomIntBetween(0, 10);
            List<JvmStats.MemoryPool> memoryPools = new ArrayList<>(numMemoryPools);
            for (int i = 0; i < numMemoryPools; i++) {
                memoryPools.add(
                    new JvmStats.MemoryPool(
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        new JvmStats.MemoryPoolGcStats(randomNonNegativeLong(), randomNonNegativeLong())
                    )
                );
            }
            JvmStats.Threads threads = new JvmStats.Threads(randomIntBetween(1, 1000), randomIntBetween(1, 1000));
            int numGarbageCollectors = randomIntBetween(0, 10);
            JvmStats.GarbageCollector[] garbageCollectorsArray = new JvmStats.GarbageCollector[numGarbageCollectors];
            for (int i = 0; i < numGarbageCollectors; i++) {
                garbageCollectorsArray[i] = new JvmStats.GarbageCollector(
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
            }
            JvmStats.GarbageCollectors garbageCollectors = new JvmStats.GarbageCollectors(garbageCollectorsArray);
            int numBufferPools = randomIntBetween(0, 10);
            List<JvmStats.BufferPool> bufferPoolList = new ArrayList<>();
            for (int i = 0; i < numBufferPools; i++) {
                bufferPoolList.add(
                    new JvmStats.BufferPool(
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    )
                );
            }
            JvmStats.Classes classes = new JvmStats.Classes(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
            jvmStats = frequently()
                ? new JvmStats(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    new JvmStats.Mem(
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        memoryPools
                    ),
                    threads,
                    garbageCollectors,
                    randomBoolean() ? Collections.emptyList() : bufferPoolList,
                    classes
                )
                : null;
        }
        ThreadPoolStats threadPoolStats = null;
        if (frequently()) {
            int numThreadPoolStats = randomIntBetween(0, 10);
            List<ThreadPoolStats.Stats> threadPoolStatsList = new ArrayList<>();
            for (int i = 0; i < numThreadPoolStats; i++) {
                threadPoolStatsList.add(
                    new ThreadPoolStats.Stats(
                        randomAlphaOfLengthBetween(3, 10),
                        randomIntBetween(1, 1000),
                        randomIntBetween(1, 1000),
                        randomIntBetween(1, 1000),
                        randomNonNegativeLong(),
                        randomIntBetween(1, 1000),
                        randomIntBetween(1, 1000)
                    )
                );
            }
            threadPoolStats = new ThreadPoolStats(threadPoolStatsList);
        }
        FsInfo fsInfo = null;
        if (frequently()) {
            int numDeviceStats = randomIntBetween(0, 10);
            FsInfo.DeviceStats[] deviceStatsArray = new FsInfo.DeviceStats[numDeviceStats];
            for (int i = 0; i < numDeviceStats; i++) {
                FsInfo.DeviceStats previousDeviceStats = randomBoolean()
                    ? null
                    : new FsInfo.DeviceStats(
                        randomInt(),
                        randomInt(),
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        null
                    );
                deviceStatsArray[i] = new FsInfo.DeviceStats(
                    randomInt(),
                    randomInt(),
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    previousDeviceStats
                );
            }
            FsInfo.IoStats ioStats = new FsInfo.IoStats(deviceStatsArray);
            int numPaths = randomIntBetween(0, 10);
            FsInfo.Path[] paths = new FsInfo.Path[numPaths];
            for (int i = 0; i < numPaths; i++) {
                paths[i] = new FsInfo.Path(
                    randomAlphaOfLengthBetween(3, 10),
                    randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null,
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
            }
            fsInfo = new FsInfo(randomNonNegativeLong(), ioStats, paths);
        }
        TransportStats transportStats = frequently()
            ? new TransportStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            )
            : null;
        HttpStats httpStats = frequently() ? new HttpStats(randomNonNegativeLong(), randomNonNegativeLong()) : null;
        AllCircuitBreakerStats allCircuitBreakerStats = null;
        if (frequently()) {
            int numCircuitBreakerStats = randomIntBetween(0, 10);
            CircuitBreakerStats[] circuitBreakerStatsArray = new CircuitBreakerStats[numCircuitBreakerStats];
            for (int i = 0; i < numCircuitBreakerStats; i++) {
                circuitBreakerStatsArray[i] = new CircuitBreakerStats(
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomDouble(),
                    randomNonNegativeLong()
                );
            }
            allCircuitBreakerStats = new AllCircuitBreakerStats(circuitBreakerStatsArray);
        }
        ScriptStats scriptStats = frequently()
            ? new ScriptStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            : null;
        DiscoveryStats discoveryStats = frequently()
            ? new DiscoveryStats(
                randomBoolean() ? new PendingClusterStateStats(randomInt(), randomInt(), randomInt()) : null,
                randomBoolean()
                    ? new PublishClusterStateStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
                    : null
            )
            : null;
        IngestStats ingestStats = null;
        if (frequently()) {
            IngestStats.Stats totalStats = new IngestStats.Stats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
            int numPipelines = randomIntBetween(0, 10);
            int numProcessors = randomIntBetween(0, 10);
            List<IngestStats.PipelineStat> ingestPipelineStats = new ArrayList<>(numPipelines);
            Map<String, List<IngestStats.ProcessorStat>> ingestProcessorStats = new HashMap<>(numPipelines);
            for (int i = 0; i < numPipelines; i++) {
                String pipelineId = randomAlphaOfLengthBetween(3, 10);
                ingestPipelineStats.add(
                    new IngestStats.PipelineStat(
                        pipelineId,
                        new IngestStats.Stats(
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong()
                        )
                    )
                );

                List<IngestStats.ProcessorStat> processorPerPipeline = new ArrayList<>(numProcessors);
                for (int j = 0; j < numProcessors; j++) {
                    IngestStats.Stats processorStats = new IngestStats.Stats(
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    );
                    processorPerPipeline.add(
                        new IngestStats.ProcessorStat(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), processorStats)
                    );
                }
                ingestProcessorStats.put(pipelineId, processorPerPipeline);
            }
            ingestStats = new IngestStats(totalStats, ingestPipelineStats, ingestProcessorStats);
        }
        AdaptiveSelectionStats adaptiveSelectionStats = null;
        if (frequently()) {
            int numNodes = randomIntBetween(0, 10);
            Map<String, Long> nodeConnections = new HashMap<>();
            Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = new HashMap<>();
            for (int i = 0; i < numNodes; i++) {
                String nodeId = randomAlphaOfLengthBetween(3, 10);
                // add outgoing connection info
                if (frequently()) {
                    nodeConnections.put(nodeId, randomLongBetween(0, 100));
                }
                // add node calculations
                if (frequently()) {
                    ResponseCollectorService.ComputedNodeStats stats = new ResponseCollectorService.ComputedNodeStats(
                        nodeId,
                        randomIntBetween(1, 10),
                        randomIntBetween(0, 2000),
                        randomDoubleBetween(1.0, 10000000.0, true),
                        randomDoubleBetween(1.0, 10000000.0, true)
                    );
                    nodeStats.put(nodeId, stats);
                }
            }
            adaptiveSelectionStats = new AdaptiveSelectionStats(nodeConnections, nodeStats);
        }
        ClusterManagerThrottlingStats clusterManagerThrottlingStats = null;
        if (frequently()) {
            clusterManagerThrottlingStats = new ClusterManagerThrottlingStats();
            clusterManagerThrottlingStats.onThrottle("test-task", randomInt());
        }
        ScriptCacheStats scriptCacheStats = scriptStats != null ? scriptStats.toScriptCacheStats() : null;

        FailOpenWeightedRoutingStats failOpenWeightedRoutingStats = null;
        failOpenWeightedRoutingStats = new FailOpenWeightedRoutingStats();
        failOpenWeightedRoutingStats.updateFailOpenCount();

        // TODO NodeIndicesStats are not tested here, way too complicated to create, also they need to be migrated to Writeable yet
        return new NodeStats(
            node,
            randomNonNegativeLong(),
            null,
            osStats,
            processStats,
            jvmStats,
            threadPoolStats,
            fsInfo,
            transportStats,
            httpStats,
            allCircuitBreakerStats,
            scriptStats,
            discoveryStats,
            ingestStats,
            adaptiveSelectionStats,
            scriptCacheStats,
            null,
            null,
            null,
            clusterManagerThrottlingStats,
            failOpenWeightedRoutingStats
        );
    }

    private IngestStats.Stats getPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream().filter(p1 -> p1.getPipelineId().equals(id)).findFirst().map(p2 -> p2.getStats()).orElse(null);
    }
}
