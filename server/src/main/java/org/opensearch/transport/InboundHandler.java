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

package org.opensearch.transport;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.nativeprotocol.NativeInboundMessage;
import org.opensearch.transport.protobufprotocol.ProtobufInboundMessage;
import org.opensearch.transport.protobufprotocol.ProtobufMessageHandler;

import java.io.IOException;
import java.util.Map;

/**
 * Handler for inbound data
 *
 * @opensearch.internal
 */
public class InboundHandler {

    private final ThreadPool threadPool;

    private volatile TransportMessageListener messageListener = TransportMessageListener.NOOP_LISTENER;

    private volatile long slowLogThresholdMs = Long.MAX_VALUE;

    private final Map<String, ProtocolMessageHandler> protocolMessageHandlers;

    InboundHandler(
        ThreadPool threadPool,
        OutboundHandler outboundHandler,
        NamedWriteableRegistry namedWriteableRegistry,
        TransportHandshaker handshaker,
        TransportKeepAlive keepAlive,
        Transport.RequestHandlers requestHandlers,
        Transport.ResponseHandlers responseHandlers,
        Tracer tracer
    ) {
        this.threadPool = threadPool;
        if (FeatureFlags.isEnabled(FeatureFlags.PROTOBUF_SETTING)) {
            this.protocolMessageHandlers = Map.of(
                ProtobufInboundMessage.PROTOBUF_PROTOCOL,
                new ProtobufMessageHandler(threadPool, responseHandlers),
                NativeInboundMessage.NATIVE_PROTOCOL,
                new NativeMessageHandler(
                    threadPool,
                    outboundHandler,
                    namedWriteableRegistry,
                    handshaker,
                    requestHandlers,
                    responseHandlers,
                    tracer,
                    keepAlive
                )
            );
        } else {
            this.protocolMessageHandlers = Map.of(
                NativeInboundMessage.NATIVE_PROTOCOL,
                new NativeMessageHandler(
                    threadPool,
                    outboundHandler,
                    namedWriteableRegistry,
                    handshaker,
                    requestHandlers,
                    responseHandlers,
                    tracer,
                    keepAlive
                )
            );
        }
    }

    void setMessageListener(TransportMessageListener listener) {
        if (messageListener == TransportMessageListener.NOOP_LISTENER) {
            messageListener = listener;
        } else {
            throw new IllegalStateException("Cannot set message listener twice");
        }
    }

    void setSlowLogThreshold(TimeValue slowLogThreshold) {
        this.slowLogThresholdMs = slowLogThreshold.getMillis();
    }

    void inboundMessage(TcpChannel channel, ProtocolInboundMessage message) throws Exception {
        final long startTime = threadPool.relativeTimeInMillis();
        channel.getChannelStats().markAccessed(startTime);
        messageReceivedFromPipeline(channel, message, startTime);
    }

    private void messageReceivedFromPipeline(TcpChannel channel, ProtocolInboundMessage message, long startTime) throws IOException {
        ProtocolMessageHandler protocolMessageHandler = protocolMessageHandlers.get(message.getProtocol());
        if (protocolMessageHandler == null) {
            throw new IllegalStateException("No protocol message handler found for protocol: " + message.getProtocol());
        }
        protocolMessageHandler.messageReceived(channel, message, startTime, slowLogThresholdMs, messageListener);
    }
}
