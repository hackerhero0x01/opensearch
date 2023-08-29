/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.noop;

import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanScope;

/**
 * No-op implementation of {@link SpanScope}
 */
public class NoopSpanScope implements SpanScope {
    /**
     * Constructor.
     */
    public NoopSpanScope() {

    }

    @Override
    public void close() {

    }

    @Override
    public Span getSpan() {
        return NoopSpan.INSTANCE;
    }
}
