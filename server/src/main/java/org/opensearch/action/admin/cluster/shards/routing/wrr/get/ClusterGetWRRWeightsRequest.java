/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards.routing.wrr.get;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request to get weights for weighted round-robin search routing policy.
 *
 * @opensearch.internal
 */
public class ClusterGetWRRWeightsRequest extends ClusterManagerNodeReadRequest<ClusterGetWRRWeightsRequest> {
    String awarenessAttribute;

    public String getAwarenessAttribute() {
        return awarenessAttribute;
    }

    public void setAwarenessAttribute(String awarenessAttribute) {
        this.awarenessAttribute = awarenessAttribute;
    }

    public ClusterGetWRRWeightsRequest() {}

    public ClusterGetWRRWeightsRequest(StreamInput in) throws IOException {
        super(in);
        awarenessAttribute = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

        super.writeTo(out);
        out.writeString(awarenessAttribute);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
