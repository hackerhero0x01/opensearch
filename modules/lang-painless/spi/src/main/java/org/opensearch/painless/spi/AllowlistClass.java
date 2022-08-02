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

package org.opensearch.painless.spi;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Class represents the equivalent of a Java class in Painless complete with super classes,
 * constructors, methods, and fields. There must be a one-to-one mapping of class names to Java
 * classes. Though, since multiple allowlists may be combined into a single allowlist for a
 * specific context, as long as multiple classes representing the same Java class have the same
 * class name and have legal constructor/method overloading they can be merged together.
 *
 * Classes in Painless allow for arity overloading for constructors and methods. Arity overloading
 * means that multiple constructors are allowed for a single class as long as they have a different
 * number of parameters, and multiples methods with the same name are allowed for a single class
 * as long as they have the same return type and a different number of parameters.
 *
 * Classes will automatically extend other allowlisted classes if the Java class they represent is a
 * subclass of other classes including Java interfaces.
 */
public final class AllowlistClass extends WhitelistClass {

    /** The {@link List} of allowlisted ({@link WhitelistConstructor}s) available to this class. */
    public final List<AllowlistConstructor> allowlistConstructors;

    /** The {@link List} of allowlisted ({@link WhitelistMethod}s) available to this class. */
    public final List<AllowlistMethod> allowlistMethods;

    /** The {@link List} of allowlisted ({@link WhitelistField}s) available to this class. */
    public final List<AllowlistField> allowlistFields;

    /** Standard constructor. All values must be not {@code null}. */
    public AllowlistClass(
        String origin,
        String javaClassName,
        List<AllowlistConstructor> allowlistConstructors,
        List<AllowlistMethod> allowlistMethods,
        List<AllowlistField> allowlistFields,
        List<Object> painlessAnnotations
    ) {
        super(
            origin,
            javaClassName,
            allowlistConstructors.stream().map(e -> (WhitelistConstructor) e).collect(Collectors.toList()),
            allowlistMethods.stream().map(e -> (WhitelistMethod) e).collect(Collectors.toList()),
            allowlistFields.stream().map(e -> (WhitelistField) e).collect(Collectors.toList()),
            painlessAnnotations
        );

        this.allowlistConstructors = Collections.unmodifiableList(Objects.requireNonNull(allowlistConstructors));
        this.allowlistMethods = Collections.unmodifiableList(Objects.requireNonNull(allowlistMethods));
        this.allowlistFields = Collections.unmodifiableList(Objects.requireNonNull(allowlistFields));
    }
}
