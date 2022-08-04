/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.util.SetOnce;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * This is an {@link Engine} abstraction intended for replica shards when Segment Replication
 * is enabled. This Engine which implements this abstract engine does not create an IndexWriter,
 * rather it refreshes a {@link NRTReplicationReaderManager} with new Segments when received from an external source.
 * The abstraction also helps to provide a different translog manager instance depending on the usecase like no-op
 * replication while having remote store enabled on top of segment replication.
 *
 * @opensearch.internal
 */
public abstract class AbstractNRTReplicationEngine extends Engine {

    private volatile SegmentInfos lastCommittedSegmentInfos;
    private final NRTReplicationReaderManager readerManager;
    private final CompletionStatsCache completionStatsCache;
    private final LocalCheckpointTracker localCheckpointTracker;
    private final SetOnce<TranslogManager> translogManager = new SetOnce<>();

    public AbstractNRTReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
        store.incRef();
        NRTReplicationReaderManager readerManager = null;
        TranslogManager translogManagerRef = null;
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
            readerManager = new NRTReplicationReaderManager(OpenSearchDirectoryReader.wrap(getDirectoryReader(), shardId));
            final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                this.lastCommittedSegmentInfos.getUserData().entrySet()
            );
            this.localCheckpointTracker = new LocalCheckpointTracker(commitInfo.maxSeqNo, commitInfo.localCheckpoint);
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            this.readerManager = readerManager;
            this.readerManager.addListener(completionStatsCache);
            for (ReferenceManager.RefreshListener listener : engineConfig.getExternalRefreshListener()) {
                this.readerManager.addListener(listener);
            }
            for (ReferenceManager.RefreshListener listener : engineConfig.getInternalRefreshListener()) {
                this.readerManager.addListener(listener);
            }
            final Map<String, String> userData = store.readLastCommittedSegmentsInfo().getUserData();
            final String translogUUID = Objects.requireNonNull(userData.get(Translog.TRANSLOG_UUID_KEY));
            translogManagerRef = createTranslogManager(translogUUID, translogManager);
            translogManager.set(translogManagerRef);
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(store::decRef, readerManager);
            closeIfCloseable(IOUtils::closeWhileHandlingException, translogManagerRef);
            throw new EngineCreationFailureException(shardId, "failed to create engine", e);
        }
    }

    protected abstract TranslogManager createTranslogManager(String translogUUID, SetOnce<TranslogManager> translogManager)
        throws IOException;

    @Override
    public TranslogManager translogManager() {
        return translogManager.get();
    }

    public synchronized void updateSegments(final SegmentInfos infos, long seqNo) throws IOException {
        // Update the current infos reference on the Engine's reader.
        readerManager.updateSegments(infos);

        // only update the persistedSeqNo and "lastCommitted" infos reference if the incoming segments have a higher
        // generation. We can still refresh with incoming SegmentInfos that are not part of a commit point.
        if (infos.getGeneration() > lastCommittedSegmentInfos.getGeneration()) {
            this.lastCommittedSegmentInfos = infos;
            translogManager().rollTranslogGeneration();
        }
        localCheckpointTracker.fastForwardProcessedSeqNo(seqNo);
    }

    @Override
    public String getHistoryUUID() {
        return loadHistoryUUID(lastCommittedSegmentInfos.userData);
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return completionStatsCache.get(fieldNamePatterns);
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public GetResult get(Get get, BiFunction<String, SearcherScope, Searcher> searcherFactory) throws EngineException {
        return getFromSearcher(get, searcherFactory, SearcherScope.EXTERNAL);
    }

    @Override
    protected ReferenceManager<OpenSearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return readerManager;
    }

    /**
     * Refreshing of this engine will only happen internally when a new set of segments is received.  The engine will ignore external
     * refresh attempts so we can return false here.  Further Engine's existing implementation reads DirectoryReader.isCurrent after acquiring a searcher.
     * With this Engine's NRTReplicationReaderManager, This will use StandardDirectoryReader's implementation which determines if the reader is current by
     * comparing the on-disk SegmentInfos version against the one in the reader, which at refresh points will always return isCurrent false and then refreshNeeded true.
     * Even if this method returns refresh as needed, we ignore it and only ever refresh with incoming SegmentInfos.
     */
    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int countNumberOfHistoryOperations(String source, long fromSeqNo, long toSeqNumber) throws IOException {
        return 0;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return false;
    }

    @Override
    public long getMinRetainedSeqNo() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return localCheckpointTracker.getPersistedCheckpoint();
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return localCheckpointTracker.getProcessedCheckpoint();
    }

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return localCheckpointTracker.getStats(globalCheckpoint);
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments(boolean verbose) {
        return Arrays.asList(getSegmentInfo(getLatestSegmentInfos(), verbose));
    }

    @Override
    public void refresh(String source) throws EngineException {}

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        return false;
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {}

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    public void flush(boolean force, boolean waitIfOngoing) throws EngineException {}

    @Override
    public void forceMerge(
        boolean flush,
        int maxNumSegments,
        boolean onlyExpungeDeletes,
        boolean upgrade,
        boolean upgradeOnlyAncientSegments,
        String forceMergeUUID
    ) throws EngineException, IOException {}

    @Override
    public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        try {
            final IndexCommit indexCommit = Lucene.getIndexCommit(lastCommittedSegmentInfos, store.directory());
            return new GatedCloseable<>(indexCommit, () -> {});
        } catch (IOException e) {
            throw new EngineException(shardId, "Unable to build latest IndexCommit", e);
        }
    }

    @Override
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        return acquireLastIndexCommit(false);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(localCheckpointTracker.getProcessedCheckpoint(), lastCommittedSegmentInfos.totalMaxDoc());
    }

    @Override
    protected final void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            assert rwl.isWriteLockedByCurrentThread() || failEngineLock.isHeldByCurrentThread()
                : "Either the write lock must be held or the engine must be currently be failing itself";
            try {
                IOUtils.close(readerManager, store::decRef);
                closeIfCloseableOrThrowsException(IOUtils::close, translogManager.get());
            } catch (Exception e) {
                logger.warn("failed to close engine", e);
            } finally {
                logger.debug("engine closed [{}]", reason);
                closedLatch.countDown();
            }
        }
    }

    private void closeIfCloseable(Consumer<Closeable> closeableConsumer, Object object) {
        if (object instanceof Closeable) {
            closeableConsumer.accept((Closeable) object);
        }
    }

    private void closeIfCloseableOrThrowsException(CheckedConsumer<Closeable, IOException> closeableConsumer, Object object)
        throws IOException {
        if (object instanceof Closeable) {
            closeableConsumer.accept((Closeable) object);
        }
    }

    @Override
    public void activateThrottling() {}

    @Override
    public void deactivateThrottling() {}

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void maybePruneDeletes() {}

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {}

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        return localCheckpointTracker.getMaxSeqNo();
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {}

    @Override
    protected SegmentInfos getLastCommittedSegmentInfos() {
        return lastCommittedSegmentInfos;
    }

    @Override
    protected SegmentInfos getLatestSegmentInfos() {
        return readerManager.getSegmentInfos();
    }

    protected LocalCheckpointTracker getLocalCheckpointTracker() {
        return localCheckpointTracker;
    }

    private DirectoryReader getDirectoryReader() throws IOException {
        // for segment replication: replicas should create the reader from store, we don't want an open IW on replicas.
        return new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(store.directory()), Lucene.SOFT_DELETES_FIELD);
    }
}
