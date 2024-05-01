/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.tasks.resourcetracker.TaskResourceInfo;
import org.opensearch.plugin.insights.rules.model.Attribute;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * information related to search queries
 *
 * @opensearch.internal
 */
public class QueryInsightsService extends AbstractLifecycleComponent {
    /**
     * The internal OpenSearch thread pool that execute async processing and exporting tasks
     */
    private final ThreadPool threadPool;

    /**
     * Services to capture top n queries for different metric types
     */
    private final Map<MetricType, TopQueriesService> topQueriesServices;

    /**
     * Flags for enabling insight data collection for different metric types
     */
    private final Map<MetricType, Boolean> enableCollect;

    /**
     * The internal thread-safe queue to ingest the search query data and subsequently forward to processors
     */
    private final LinkedBlockingQueue<SearchQueryRecord> queryRecordsQueue;

    /**
     * The internal thread-safe queue to ingest the task-level resource usage data
     */
    public final LinkedBlockingQueue<TaskResourceInfo> taskRecordsQueue = new LinkedBlockingQueue<>();

    /**
     * This map keeps track of the status of tasks associated with a query.
     * - The value of the entry is 0: all tasks associated with the query has completed.
     * - The value of the entry is greater than 0: Some tasks related to this query have not been completed yet.
     */
    public final ConcurrentHashMap<Long, AtomicInteger> taskStatusMap = new ConcurrentHashMap<>();

    /**
     * Holds a reference to delayed operation {@link Scheduler.Cancellable} so it can be cancelled when
     * the service closed concurrently.
     */
    protected volatile Scheduler.Cancellable scheduledFuture;

    /**
     * Constructor of the QueryInsightsService
     *
     * @param threadPool     The OpenSearch thread pool to run async tasks
     */
    @Inject
    public QueryInsightsService(final ThreadPool threadPool) {
        enableCollect = new HashMap<>();
        queryRecordsQueue = new LinkedBlockingQueue<>(QueryInsightsSettings.QUERY_RECORD_QUEUE_CAPACITY);
        topQueriesServices = new HashMap<>();
        for (MetricType metricType : MetricType.allMetricTypes()) {
            enableCollect.put(metricType, false);
            topQueriesServices.put(metricType, new TopQueriesService(metricType));
        }
        this.threadPool = threadPool;
    }

    /**
     * Ingest the query data into in-memory stores
     *
     * @param record the record to ingest
     */
    public boolean addRecord(final SearchQueryRecord record) {
        boolean shouldAdd = false;
        for (Map.Entry<MetricType, TopQueriesService> entry : topQueriesServices.entrySet()) {
            if (!enableCollect.get(entry.getKey())) {
                continue;
            }
            List<SearchQueryRecord> currentSnapshot = entry.getValue().getTopQueriesCurrentSnapshot();
            // skip add to top N queries store if the incoming record is smaller than the Nth record
            if (currentSnapshot.size() < entry.getValue().getTopNSize()
                || SearchQueryRecord.compare(record, currentSnapshot.get(0), entry.getKey()) > 0) {
                shouldAdd = true;
                break;
            }
        }
        if (shouldAdd) {
            return queryRecordsQueue.offer(record);
        }
        return false;
    }

    /**
     * Drain the queryRecordsQueue into internal stores and services
     */
    public void drainRecords() {
        final List<SearchQueryRecord> queryRecords = new ArrayList<>();
        final List<TaskResourceInfo> taskRecords = new ArrayList<>();
        queryRecordsQueue.drainTo(queryRecords);
        taskRecordsQueue.drainTo(taskRecords);
        final List<SearchQueryRecord> finishedQueryRecord = correlateTasks(queryRecords, taskRecords);
        finishedQueryRecord.sort(Comparator.comparingLong(SearchQueryRecord::getTimestamp));
        for (MetricType metricType : MetricType.allMetricTypes()) {
            if (enableCollect.get(metricType)) {
                // ingest the records into topQueriesService
                topQueriesServices.get(metricType).consumeRecords(finishedQueryRecord);
            }
        }
    }

    private List<SearchQueryRecord> correlateTasks(List<SearchQueryRecord> queryRecords, List<TaskResourceInfo> taskRecords) {
        List<SearchQueryRecord> finishedRecords = new ArrayList<>();
        // group taskRecords by parent task
        Map<Long, List<TaskResourceInfo>> taskIdToResources = taskRecords.stream()
            .collect(Collectors.groupingBy(TaskResourceInfo::getParentTaskId, HashMap::new, Collectors.toList()));

        for (SearchQueryRecord record : queryRecords) {
            long taskId = (long) record.getAttributes().get(Attribute.TASK_ID);
            if (!taskStatusMap.containsKey(taskId)) {
                // write back since there's no task information.
                queryRecordsQueue.offer(record);
                continue;
            }
            // parent task has finished
            if (taskStatusMap.get(taskId).get() == 0) {
                long cpuUsage = taskIdToResources.get(taskId)
                    .stream()
                    .map(r -> r.getTaskResourceUsage().getCpuTimeInNanos())
                    .reduce(0L, Long::sum);
                long memUsage = taskIdToResources.get(taskId)
                    .stream()
                    .map(r -> r.getTaskResourceUsage().getMemoryInBytes())
                    .reduce(0L, Long::sum);
                record.addMeasurement(MetricType.CPU, cpuUsage);
                record.addMeasurement(MetricType.MEMORY, memUsage);
                finishedRecords.add(record);
            } else {
                // write back since the task has not complete
                queryRecordsQueue.offer(record);
                taskRecordsQueue.addAll(taskIdToResources.get(taskId));
            }
        }
        return finishedRecords;
    }

    /**
     * Get the top queries service based on metricType
     * @param metricType {@link MetricType}
     * @return {@link TopQueriesService}
     */
    public TopQueriesService getTopQueriesService(final MetricType metricType) {
        return topQueriesServices.get(metricType);
    }

    /**
     * Set flag to enable or disable Query Insights data collection
     *
     * @param metricType {@link MetricType}
     * @param enable Flag to enable or disable Query Insights data collection
     */
    public void enableCollection(final MetricType metricType, final boolean enable) {
        this.enableCollect.put(metricType, enable);
        this.topQueriesServices.get(metricType).setEnabled(enable);
    }

    /**
     * Get if the Query Insights data collection is enabled for a MetricType
     *
     * @param metricType {@link MetricType}
     * @return if the Query Insights data collection is enabled
     */
    public boolean isCollectionEnabled(final MetricType metricType) {
        return this.enableCollect.get(metricType);
    }

    /**
     * Check if query insights service is enabled
     *
     * @return if query insights service is enabled
     */
    public boolean isEnabled() {
        for (MetricType t : MetricType.allMetricTypes()) {
            if (isCollectionEnabled(t)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void doStart() {
        if (isEnabled()) {
            scheduledFuture = threadPool.scheduleWithFixedDelay(
                this::drainRecords,
                QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL,
                QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR
            );
        }
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {}
}
