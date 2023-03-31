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

package org.opensearch.search.aggregations.metrics;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.*;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Aggregation Factory for sum agg
 *
 * @opensearch.internal
 */
class SumAggregatorFactory extends ValuesSourceAggregatorFactory {

    SumAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            SumAggregationBuilder.REGISTRY_KEY,
            List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN),
            SumAggregatorFactory::methodName,
            true
        );
    }

    private static Aggregator methodName(String s, ValuesSourceConfig valuesSourceConfig, SearchContext searchContext, Aggregator aggregator, Map<String, Object> stringObjectMap) throws IOException {
        if (valuesSourceConfig.hasValues()) {
            if (((ValuesSource.Numeric) valuesSourceConfig.getValuesSource()).isFloatingPoint()) {
                return new SumAggregator(s, valuesSourceConfig, searchContext, aggregator, stringObjectMap);
            } else {
                return new SumIntegralAggregator(s, valuesSourceConfig, searchContext, aggregator, stringObjectMap);
            }
        } else {
            return new SumNullAggregator(s, valuesSourceConfig, searchContext, aggregator, stringObjectMap);
        }
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext, Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new SumAggregator(name, config, searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound bucketCardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(SumAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config, searchContext, parent, metadata);
    }
}
