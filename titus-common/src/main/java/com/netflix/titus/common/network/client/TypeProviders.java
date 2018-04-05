/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.network.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.titus.common.network.client.internal.ClassTypeProvider;
import com.netflix.titus.common.network.client.internal.JacksonTypeReferenceProvider;
import com.netflix.titus.common.network.client.internal.RxHttpResponseTypeProvider;

/**
 * Type information is needed for object parsing. As generic type information is erased at runtime,
 * different libraries provide own type reference implementations (for example Jackson comes with {@link TypeReference}).
 * {@link RxRestClient.TypeProvider} interface provides weak abstraction, as it exposes directly Jackson API, and it is a compromise
 * to keep the implementation simple.
 */
public class TypeProviders {

    public static <T> RxRestClient.TypeProvider<T> of(Class<T> entityClass) {
        return new ClassTypeProvider<T>(entityClass);
    }

    public static <T> RxRestClient.TypeProvider<T> of(TypeReference<T> jacksonTypeReference) {
        return new JacksonTypeReferenceProvider<>(jacksonTypeReference);
    }

    public static <T> RxRestClient.TypeProvider<RxHttpResponse<T>> ofResponse(Class<T> entityClass) {
        return new RxHttpResponseTypeProvider<>(entityClass);
    }

    public static <T> RxRestClient.TypeProvider<RxHttpResponse<T>> ofResponse(TypeReference<T> jacksonTypeReference) {
        return new RxHttpResponseTypeProvider<>(jacksonTypeReference);
    }

    public static <O> RxRestClient.TypeProvider<RxHttpResponse<O>> ofResponse(RxRestClient.TypeProvider<O> replyType) {
        return new RxHttpResponseTypeProvider<>(replyType);
    }

    public static RxRestClient.TypeProvider<RxHttpResponse<Void>> ofEmptyResponse() {
        return new RxHttpResponseTypeProvider<>(RxHttpResponseTypeProvider.VOID_TYPE_PROVIDER);
    }
}
