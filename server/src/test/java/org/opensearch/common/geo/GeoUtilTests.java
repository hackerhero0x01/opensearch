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

package org.opensearch.common.geo;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class GeoUtilTests extends OpenSearchTestCase {

    public void testPrecisionParser() throws IOException {
        assertEquals(10, parsePrecision(builder -> builder.field("test", 10)));
        assertEquals(10, parsePrecision(builder -> builder.field("test", 10.2)));
        assertEquals(6, parsePrecision(builder -> builder.field("test", "6")));
        assertEquals(7, parsePrecision(builder -> builder.field("test", "1km")));
        assertEquals(7, parsePrecision(builder -> builder.field("test", "1.1km")));
    }

    public void testIncorrectPrecisionParser() {
        expectThrows(NumberFormatException.class, () -> parsePrecision(builder -> builder.field("test", "10.1.1.1")));
        expectThrows(NumberFormatException.class, () -> parsePrecision(builder -> builder.field("test", "364.4smoots")));
        assertEquals(
            "precision too high [0.01mm]",
            expectThrows(IllegalArgumentException.class, () -> parsePrecision(builder -> builder.field("test", "0.01mm"))).getMessage()
        );
    }

    /**
     * Invokes GeoUtils.parsePrecision parser on the value generated by tokenGenerator
     * <p>
     * The supplied tokenGenerator should generate a single field that contains the precision in
     * one of the supported formats or malformed precision value if error handling is tested. The
     * method return the parsed value or throws an exception, if precision value is malformed.
     */
    private int parsePrecision(CheckedConsumer<XContentBuilder, IOException> tokenGenerator) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        tokenGenerator.accept(builder);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken()); // {
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken()); // field name
            assertTrue(parser.nextToken().isValue()); // field value
            int precision = GeoUtils.parsePrecision(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken()); // }
            assertNull(parser.nextToken()); // no more tokens
            return precision;
        }
    }
}
