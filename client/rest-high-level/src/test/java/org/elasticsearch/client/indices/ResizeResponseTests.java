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

package org.elasticsearch.client.indices;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class ResizeResponseTests extends
    AbstractResponseTestCase<org.opensearch.action.admin.indices.shrink.ResizeResponse, ResizeResponse> {

    @Override
    protected org.opensearch.action.admin.indices.shrink.ResizeResponse createServerTestInstance(XContentType xContentType) {
        boolean acked = randomBoolean();
        return new org.opensearch.action.admin.indices.shrink.ResizeResponse(acked, acked, randomAlphaOfLength(5));
    }

    @Override
    protected ResizeResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return ResizeResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.opensearch.action.admin.indices.shrink.ResizeResponse serverTestInstance,
                                   ResizeResponse clientInstance) {
        assertEquals(serverTestInstance.isAcknowledged(), clientInstance.isAcknowledged());
        assertEquals(serverTestInstance.isShardsAcknowledged(), clientInstance.isShardsAcknowledged());
        assertEquals(serverTestInstance.index(), clientInstance.index());
    }
}
