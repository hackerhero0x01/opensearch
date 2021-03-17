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

package org.opensearch.action.ingest;

import org.opensearch.action.ActionRequestBuilder;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentType;

public class PutPipelineRequestBuilder extends ActionRequestBuilder<PutPipelineRequest, AcknowledgedResponse> {

    public PutPipelineRequestBuilder(OpenSearchClient client, PutPipelineAction action) {
        super(client, action, new PutPipelineRequest());
    }

    public PutPipelineRequestBuilder(OpenSearchClient client, PutPipelineAction action, String id, BytesReference source,
                                     XContentType xContentType) {
        super(client, action, new PutPipelineRequest(id, source, xContentType));
    }
}
