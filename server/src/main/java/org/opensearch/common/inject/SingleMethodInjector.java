/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.inject;

import org.opensearch.common.inject.InjectorImpl.MethodInvoker;
import org.opensearch.common.inject.internal.Errors;
import org.opensearch.common.inject.internal.ErrorsException;
import org.opensearch.common.inject.internal.InternalContext;
import org.opensearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Invokes an injectable method.
 *
 * @opensearch.internal
 */
class SingleMethodInjector implements SingleMemberInjector {
    final MethodInvoker methodInvoker;
    final SingleParameterInjector<?>[] parameterInjectors;
    final InjectionPoint injectionPoint;

    SingleMethodInjector(InjectorImpl injector, InjectionPoint injectionPoint, Errors errors) throws ErrorsException {
        this.injectionPoint = injectionPoint;
        final Method method = (Method) injectionPoint.getMember();
        methodInvoker = method::invoke;
        parameterInjectors = injector.getParametersInjectors(injectionPoint.getDependencies(), errors);
    }

    @Override
    public void inject(Errors errors, InternalContext context, Object o) {
        Object[] parameters;
        try {
            parameters = SingleParameterInjector.getAll(errors, context, parameterInjectors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
            return;
        }

        try {
            methodInvoker.invoke(o, parameters);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e); // a security manager is blocking us, we're hosed
        } catch (InvocationTargetException userException) {
            Throwable cause = userException.getCause() != null ? userException.getCause() : userException;
            errors.withSource(injectionPoint).errorInjectingMethod(cause);
        }
    }
}
