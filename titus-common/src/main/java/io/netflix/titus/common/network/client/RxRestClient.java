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

package io.netflix.titus.common.network.client;

import java.net.URI;

import rx.Observable;

/**
 * A simple facade for asynchronous REST/JSON client. An underlying implementation is based now on RxNetty 4.x,
 * and Jackson 2.x libraries.
 */
public interface RxRestClient {

    interface EndpointResolver {
        Observable<URI> resolve();
    }

    interface TypeProvider<T> {
        Class<T> getEntityClass();
    }

    <T> Observable<T> doGET(String relativeURI, TypeProvider<T> typeProvider);

    <REQ> Observable<Void> doPOST(String relativeURI, REQ entity);

    <REQ, RESP> Observable<RESP> doPOST(String relativeURI, REQ entity, TypeProvider<RESP> replyTypeProvider);

    <REQ> Observable<Void> doPUT(String relativeURI, REQ entity);

    <REQ, RESP> Observable<RESP> doPUT(String relativeURI, REQ entity, TypeProvider<RESP> replyTypeProvider);
}
