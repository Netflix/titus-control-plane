/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.network.client.internal;

import com.fasterxml.jackson.core.type.TypeReference;
import io.netflix.titus.common.network.client.RxHttpResponse;
import io.netflix.titus.common.network.client.RxRestClient;
import io.netflix.titus.common.network.client.TypeProviders;

/**
 *
 */
public class RxHttpResponseTypeProvider<T> implements RxRestClient.TypeProvider<RxHttpResponse<T>> {

    public static final RxRestClient.TypeProvider<Void> VOID_TYPE_PROVIDER = TypeProviders.of(Void.class);

    private RxRestClient.TypeProvider<T> bodyType;

    public RxHttpResponseTypeProvider(RxRestClient.TypeProvider<T> typeProvider) {
        this.bodyType = typeProvider;
    }

    public RxHttpResponseTypeProvider(TypeReference<T> typeReference) {
        this.bodyType = new JacksonTypeReferenceProvider<>(typeReference);
    }

    public RxHttpResponseTypeProvider(Class<T> entityClass) {
        this.bodyType = new ClassTypeProvider<>(entityClass);
    }

    @Override
    public Class<RxHttpResponse<T>> getEntityClass() {
        return (Class) RxHttpResponse.class;
    }

    public RxRestClient.TypeProvider<T> getBodyType() {
        return bodyType;
    }

    public boolean noBody() {
        return bodyType == VOID_TYPE_PROVIDER;
    }
}
