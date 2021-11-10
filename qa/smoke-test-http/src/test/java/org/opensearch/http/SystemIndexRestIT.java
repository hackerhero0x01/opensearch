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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.http;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class SystemIndexRestIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(SystemIndexTestPlugin.class);
        return plugins;
    }

    public void testSystemIndexAccessBlockedByDefault() throws Exception {
        // create index
        {
            Request putDocRequest = new Request("POST", "/_sys_index_test/add_doc/42");
            Response resp = getRestClient().performRequest(putDocRequest);
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(201));
        }


        // make sure the system index now exists
        assertBusy(() -> {
            Request searchRequest = new Request("GET", "/" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "/_count");
            searchRequest.setOptions(expectWarnings("this request accesses system indices: [" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME +
                "], but in a future major version, direct access to system indices will be prevented by default"));

            // Disallow no indices to cause an exception if the flag above doesn't work
            searchRequest.addParameter("allow_no_indices", "false");
            searchRequest.setJsonEntity("{\"query\": {\"match\":  {\"some_field\":  \"some_value\"}}}");

            final Response searchResponse = getRestClient().performRequest(searchRequest);
            assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
            Map<String, Object> responseMap = entityAsMap(searchResponse);
            assertThat(responseMap, hasKey("count"));
            assertThat(responseMap.get("count"), equalTo(1));
        });

        // And with a partial wildcard
        assertDeprecationWarningOnAccess(".test-*", SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // Try to index a doc directly
        {
            Request putDocDirectlyRequest = new Request("PUT", "/" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "/_doc/43");
            putDocDirectlyRequest.setJsonEntity("{\"some_field\":  \"some_other_value\"}");
            Response response = getRestClient().performRequest(putDocDirectlyRequest);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
        }
    }

    private void assertDeprecationWarningOnAccess(String queryPattern, String warningIndexName) throws IOException {
        String expectedWarning = "this request accesses system indices: [" + warningIndexName + "], but in a " +
            "future major version, direct access to system indices will be prevented by default";
        Request searchRequest = new Request("GET", "/" + queryPattern + randomFrom("/_count", "/_search"));
        searchRequest.setJsonEntity("{\"query\": {\"match\":  {\"some_field\":  \"some_value\"}}}");
        // Disallow no indices to cause an exception if this resolves to zero indices, so that we're sure it resolved the index
        searchRequest.addParameter("allow_no_indices", "false");
        searchRequest.setOptions(expectWarnings(expectedWarning));

        Response response = getRestClient().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private RequestOptions expectWarnings(String expectedWarning) {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setWarningsHandler(w -> w.contains(expectedWarning) == false || w.size() != 1);
        return builder.build();
    }


    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

        @Override
        public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                                 IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                                 Supplier<DiscoveryNodes> nodesInCluster) {
            return org.opensearch.common.collect.List.of(new AddDocRestHandler());
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return Collections.singletonList(new SystemIndexDescriptor(SYSTEM_INDEX_NAME, "System indices for tests"));
        }

        public static class AddDocRestHandler extends BaseRestHandler {
            @Override
            public boolean allowSystemIndexAccessByDefault() {
                return true;
            }

            @Override
            public String getName() {
                return "system_index_test_doc_adder";
            }

            @Override
            public List<Route> routes() {
                return org.opensearch.common.collect.List.of(new Route(RestRequest.Method.POST, "/_sys_index_test/add_doc/{id}"));
            }

            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                IndexRequest indexRequest = new IndexRequest(SYSTEM_INDEX_NAME);
                indexRequest.id(request.param("id"));
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                indexRequest.source(org.opensearch.common.collect.Map.of("some_field", "some_value"));
                return channel -> client.index(indexRequest,
                    new RestStatusToXContentListener<>(channel, r -> r.getLocation(indexRequest.routing())));
            }
        }
    }
}
