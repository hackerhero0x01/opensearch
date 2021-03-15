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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.DELETE;

public class RestDeleteIndexAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(DELETE, "/"),
            new Route(DELETE, "/{index}")));
    }

    @Override
    public String getName() {
        return "delete_index_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(Strings.splitStringByCommaToArray(request.param("index")));
        deleteIndexRequest.timeout(request.paramAsTime("timeout", deleteIndexRequest.timeout()));
        deleteIndexRequest.masterNodeTimeout(request.paramAsTime("master_timeout", deleteIndexRequest.masterNodeTimeout()));
        deleteIndexRequest.indicesOptions(IndicesOptions.fromRequest(request, deleteIndexRequest.indicesOptions()));
        return channel -> client.admin().indices().delete(deleteIndexRequest, new RestToXContentListener<>(channel));
    }
}
