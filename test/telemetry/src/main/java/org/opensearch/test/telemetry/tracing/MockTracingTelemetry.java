/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.telemetry.tracing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.TracingContextPropagator;
import org.opensearch.telemetry.tracing.TracingTelemetry;
import org.opensearch.telemetry.tracing.attributes.Attributes;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mock {@link TracingTelemetry} implementation for testing.
 */
public class MockTracingTelemetry implements TracingTelemetry {

    private final SpanProcessor spanProcessor = new StrictCheckSpanProcessor();
    private final AtomicBoolean shutdown = new AtomicBoolean();

    private final Logger logger = LogManager.getLogger(MockTracingTelemetry.class);

    /**
     * Base constructor.
     */
    public MockTracingTelemetry() {}

    @Override
    public Span createSpan(String spanName, Span parentSpan, Attributes attributes) {
        Span span = new MockSpan(spanName, parentSpan, spanProcessor, attributes);
        if (shutdown.get() == false) {
            logger.info("adding span {} {}", Thread.currentThread().getName(), span);
            spanProcessor.onStart(span);
        }
        return span;
    }

    @Override
    public TracingContextPropagator getContextPropagator() {
        return new MockTracingContextPropagator(spanProcessor);
    }

    @Override
    public void close() {
        logger.info("Telemetry close {}", Thread.currentThread().getName());
        // StrictCheckSpanProcessor.shutdown();
        shutdown.set(true);
    }

}
