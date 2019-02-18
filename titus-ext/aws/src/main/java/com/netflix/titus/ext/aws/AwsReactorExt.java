/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.ext.aws;

import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.AsyncHandler;
import reactor.core.publisher.Mono;

public class AwsReactorExt {

    public static  <REQUEST extends AmazonWebServiceRequest, RESPONSE> Mono<RESPONSE> toMono(
            Supplier<REQUEST> request,
            BiFunction<REQUEST, AsyncHandler<REQUEST, RESPONSE>, Future<RESPONSE>> callFun
    ) {
        return Mono.create(emitter -> {
            AsyncHandler<REQUEST, RESPONSE> asyncHandler = new AsyncHandler<REQUEST, RESPONSE>() {
                @Override
                public void onError(Exception exception) { emitter.error(exception); }

                @Override
                public void onSuccess(REQUEST request, RESPONSE result) { emitter.success(result); }
            };
            Future<RESPONSE> future = callFun.apply(request.get(), asyncHandler);
            emitter.onDispose(() -> {
                if (!future.isCancelled() && !future.isDone()) {
                    future.cancel(true);
                }
            });
        });
    }

    public static  <REQUEST extends AmazonWebServiceRequest, RESPONSE> Mono<RESPONSE> toMono(
            REQUEST request,
            BiFunction<REQUEST, AsyncHandler<REQUEST, RESPONSE>, Future<RESPONSE>> callFun
    ) {
        Supplier<REQUEST> supplier = () -> request;
        return toMono(supplier, callFun);
    }
}
