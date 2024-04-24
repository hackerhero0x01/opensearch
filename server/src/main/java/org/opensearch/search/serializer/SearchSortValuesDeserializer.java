/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.serializer;

import org.opensearch.search.SearchSortValues;

import java.io.IOException;

/**
 * Deserializer for {@link SearchSortValues} which can be implemented for different types of serde mechanisms.
 */
public interface SearchSortValuesDeserializer<T> {

    SearchSortValues createSearchSortValues(T inputStream) throws IOException;

}
