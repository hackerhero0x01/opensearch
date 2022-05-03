/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

/**
 * Action type for creating PIT reader context
 */
public class CreatePITAction extends ActionType<CreatePITResponse> {
    public static final CreatePITAction INSTANCE = new CreatePITAction();
    public static final String NAME = "indices:data/write/pit";

    private CreatePITAction() {
        super(NAME, CreatePITResponse::new);
    }
}
