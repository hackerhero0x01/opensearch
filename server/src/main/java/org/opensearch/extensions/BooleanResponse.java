/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportResponse;
import java.io.IOException;
import java.util.Objects;

/**
 * Generic boolean response indicating the status of some previous request sent to the SDK
 *
 * @opensearch.internal
 */
public class BooleanResponse extends TransportResponse {

    private final boolean status;

    /**
     * @param status Boolean indicating the status of the parse request sent to the SDK
     */
    public BooleanResponse(boolean status) {
        this.status = status;
    }

    public BooleanResponse(StreamInput in) throws IOException {
        super(in);
        this.status = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(status);
    }

    @Override
    public String toString() {
        return "BooleanResponse{" + "status=" + this.status + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BooleanResponse that = (BooleanResponse) o;
        return Objects.equals(this.status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }

    /**
     * Returns a boolean indicating the success of the request sent to the SDK
     */
    public boolean getStatus() {
        return this.status;
    }

}
