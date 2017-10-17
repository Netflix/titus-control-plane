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

package io.netflix.titus.master.endpoint.common;

import java.util.function.Consumer;

import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.master.endpoint.TitusServiceGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

public final class TitusServiceGatewayUtil {

    private static final Logger logger = LoggerFactory.getLogger(TitusServiceGatewayUtil.class);

    private TitusServiceGatewayUtil() {
    }

    /**
     * Try/catch all wrapper for {@link TitusServiceGateway} service methods. It is expected that exception will be
     * handled directly in the service method body, but if an exception is missed, it will be caught here and
     * reported.
     */
    public static <R> Observable<R> newObservable(Consumer<Subscriber<? super R>> consumer) {
        return Observable.unsafeCreate(subscriber -> {
            try {
                consumer.accept(subscriber);
            } catch (IllegalArgumentException e) {
                logger.debug("Invalid arguments", e);
                subscriber.onError(TitusServiceException.invalidArgument(e));
            } catch (Throwable e) {
                logger.error("Unhandled error", e);
                subscriber.onError(TitusServiceException.unexpected("Unhandled error"));
            }
        });
    }
}
