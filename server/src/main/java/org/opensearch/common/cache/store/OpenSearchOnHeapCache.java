/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.store;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.ICache;
import org.opensearch.common.cache.ICacheKey;
import org.opensearch.common.cache.LoadAwareCacheLoader;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.common.cache.RemovalNotification;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.cache.settings.CacheSettings;
import org.opensearch.common.cache.stats.CacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.cache.stats.NoopCacheStatsHolder;
import org.opensearch.common.cache.store.builders.ICacheBuilder;
import org.opensearch.common.cache.store.config.CacheConfig;
import org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongBiFunction;

import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.EXPIRE_AFTER_ACCESS_KEY;
import static org.opensearch.common.cache.store.settings.OpenSearchOnHeapCacheSettings.MAXIMUM_SIZE_IN_BYTES_KEY;

/**
 * This variant of on-heap cache uses OpenSearch custom cache implementation.
 * @param <K> Type of key.
 * @param <V> Type of value.
 *
 * @opensearch.experimental
 */
public class OpenSearchOnHeapCache<K, V> implements ICache<K, V>, RemovalListener<ICacheKey<K>, V> {

    private final Cache<ICacheKey<K>, V> cache;
    private final CacheStatsHolder cacheStatsHolder;
    private final RemovalListener<ICacheKey<K>, V> removalListener;
    private final List<String> dimensionNames;
    private final ToLongBiFunction<ICacheKey<K>, V> weigher;
    private final boolean useNoopStats;

    public OpenSearchOnHeapCache(Builder<K, V> builder) {
        CacheBuilder<ICacheKey<K>, V> cacheBuilder = CacheBuilder.<ICacheKey<K>, V>builder()
            .setMaximumWeight(builder.getMaxWeightInBytes())
            .weigher(builder.getWeigher())
            .removalListener(this);
        if (builder.getExpireAfterAcess() != null) {
            cacheBuilder.setExpireAfterAccess(builder.getExpireAfterAcess());
        }
        cache = cacheBuilder.build();
        this.dimensionNames = Objects.requireNonNull(builder.dimensionNames, "Dimension names can't be null");
        this.useNoopStats = builder.getUseNoopStats();
        if (useNoopStats) {
            this.cacheStatsHolder = NoopCacheStatsHolder.getInstance();
        } else {
            this.cacheStatsHolder = new DefaultCacheStatsHolder(dimensionNames);
        }
        this.removalListener = builder.getRemovalListener();
        this.weigher = builder.getWeigher();
    }

    @Override
    public V get(ICacheKey<K> key) {
        V value = cache.get(key);
        if (value != null) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
        }
        return value;
    }

    @Override
    public void put(ICacheKey<K> key, V value) {
        cache.put(key, value);
        cacheStatsHolder.incrementEntries(key.dimensions);
        cacheStatsHolder.incrementSizeInBytes(key.dimensions, weigher.applyAsLong(key, value));
    }

    @Override
    public V computeIfAbsent(ICacheKey<K> key, LoadAwareCacheLoader<ICacheKey<K>, V> loader) throws Exception {
        V value = cache.computeIfAbsent(key, key1 -> loader.load(key));
        if (!loader.isLoaded()) {
            cacheStatsHolder.incrementHits(key.dimensions);
        } else {
            cacheStatsHolder.incrementMisses(key.dimensions);
            cacheStatsHolder.incrementEntries(key.dimensions);
            cacheStatsHolder.incrementSizeInBytes(key.dimensions, cache.getWeigher().applyAsLong(key, value));
        }
        return value;
    }

    @Override
    public void invalidate(ICacheKey<K> key) {
        if (key.getDropStatsForDimensions()) {
            cacheStatsHolder.removeDimensions(key.dimensions);
        }
        if (key.key != null) {
            cache.invalidate(key);
        }
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        cacheStatsHolder.reset();
    }

    @Override
    public Iterable<ICacheKey<K>> keys() {
        return cache.keys();
    }

    @Override
    public long count() {
        return cache.count();
    }

    @Override
    public void refresh() {
        cache.refresh();
    }

    @Override
    public void close() {}

    @Override
    public ImmutableCacheStatsHolder stats() {
        return cacheStatsHolder.getImmutableCacheStatsHolder();
    }

    @Override
    public void onRemoval(RemovalNotification<ICacheKey<K>, V> notification) {
        removalListener.onRemoval(notification);
        cacheStatsHolder.decrementEntries(notification.getKey().dimensions);
        cacheStatsHolder.decrementSizeInBytes(
            notification.getKey().dimensions,
            cache.getWeigher().applyAsLong(notification.getKey(), notification.getValue())
        );

        if (RemovalReason.EVICTED.equals(notification.getRemovalReason())
            || RemovalReason.CAPACITY.equals(notification.getRemovalReason())) {
            cacheStatsHolder.incrementEvictions(notification.getKey().dimensions);
        }
    }

    /**
     * Factory to create OpenSearchOnheap cache.
     */
    public static class OpenSearchOnHeapCacheFactory implements Factory {

        public static final String NAME = "opensearch_onheap";

        @Override
        public <K, V> ICache<K, V> create(CacheConfig<K, V> config, CacheType cacheType, Map<String, Factory> cacheFactories) {
            Map<String, Setting<?>> settingList = OpenSearchOnHeapCacheSettings.getSettingListForCacheType(cacheType);
            Settings settings = config.getSettings();
            boolean useNoopStats = useNoopStats(config.getSettings(), config.getUseNoopStats());
            ICacheBuilder<K, V> builder = new Builder<K, V>().setDimensionNames(config.getDimensionNames())
                .setUseNoopStats(useNoopStats)
                .setMaximumWeightInBytes(((ByteSizeValue) settingList.get(MAXIMUM_SIZE_IN_BYTES_KEY).get(settings)).getBytes())
                .setExpireAfterAccess(((TimeValue) settingList.get(EXPIRE_AFTER_ACCESS_KEY).get(settings)))
                .setWeigher(config.getWeigher())
                .setRemovalListener(config.getRemovalListener());
            Setting<String> cacheSettingForCacheType = CacheSettings.CACHE_TYPE_STORE_NAME.getConcreteSettingForNamespace(
                cacheType.getSettingPrefix()
            );
            String storeName = cacheSettingForCacheType.get(settings);
            if (!FeatureFlags.PLUGGABLE_CACHE_SETTING.get(settings) || (storeName == null || storeName.isBlank())) {
                // For backward compatibility as the user intent is to use older settings.
                builder.setMaximumWeightInBytes(config.getMaxSizeInBytes());
                builder.setExpireAfterAccess(config.getExpireAfterAccess());
            }
            return builder.build();
        }

        @Override
        public String getCacheName() {
            return NAME;
        }

        private boolean useNoopStats(Settings settings, boolean configUseNoopStats) {
            // Use noop stats when pluggable caching is off, or when explicitly set in the CacheConfig
            return !FeatureFlags.PLUGGABLE_CACHE_SETTING.get(settings) || configUseNoopStats;
        }
    }

    /**
     * Builder object
     * @param <K> Type of key
     * @param <V> Type of value
     */
    public static class Builder<K, V> extends ICacheBuilder<K, V> {
        private List<String> dimensionNames;

        public Builder<K, V> setDimensionNames(List<String> dimensionNames) {
            this.dimensionNames = dimensionNames;
            return this;
        }

        @Override
        public ICache<K, V> build() {
            return new OpenSearchOnHeapCache<K, V>(this);
        }
    }
}
