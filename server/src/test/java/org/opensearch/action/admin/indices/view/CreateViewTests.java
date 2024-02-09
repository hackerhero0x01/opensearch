/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;

public class CreateViewTests extends AbstractWireSerializingTestCase<CreateViewAction.Request> {

    @Override
    protected Writeable.Reader<CreateViewAction.Request> instanceReader() {
        return CreateViewAction.Request::new;
    }

    @Override
    protected CreateViewAction.Request createTestInstance() {
        return new CreateViewAction.Request(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomList(5, () -> new CreateViewAction.Request.Target(randomAlphaOfLength(8)))
        );
    }

    public void testValidateRequest() {
        final CreateViewAction.Request request = new CreateViewAction.Request(
            "my-view",
            "this is a description",
            List.of(new CreateViewAction.Request.Target("my-indices-*"))
        );

        assertNull(request.validate());
    }

    public void testValidateRequestWithoutName() {
        final CreateViewAction.Request request = new CreateViewAction.Request("", null, null);
        ActionRequestValidationException e = request.validate();

        assertThat(e.validationErrors(), contains("name cannot be empty or null", "targets cannot be empty"));
    }

}