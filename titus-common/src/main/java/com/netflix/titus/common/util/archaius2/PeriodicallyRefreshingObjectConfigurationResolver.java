/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.common.util.archaius2;

import java.time.Duration;
import java.util.function.Function;

import com.netflix.archaius.api.Config;
import com.netflix.titus.common.util.closeable.CloseableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Spring Environment does not provide change callbacks. In this integration we refresh the data periodically using the
 * provided update trigger.
 */
class PeriodicallyRefreshingObjectConfigurationResolver<OBJECT, CONFIG> implements ObjectConfigurationResolver<OBJECT, CONFIG> {

    private static final Logger logger = LoggerFactory.getLogger(PeriodicallyRefreshingObjectConfigurationResolver.class);

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(5);

    private final Config configuration;
    private final Archaius2ObjectConfigurationResolver<OBJECT, CONFIG> delegate;

    PeriodicallyRefreshingObjectConfigurationResolver(Config configuration,
                                                      Function<OBJECT, String> selectorFieldAccessor,
                                                      Function<String, CONFIG> dynamicProxyFactory,
                                                      CONFIG defaultConfig) {
        this.configuration = configuration;
        this.delegate = new Archaius2ObjectConfigurationResolver<>(
                configuration,
                selectorFieldAccessor,
                dynamicProxyFactory,
                defaultConfig
        );
    }

    @Override
    public CONFIG resolve(OBJECT object) {
        return delegate.resolve(object);
    }

    void refresh() {
        try {
            delegate.onConfigUpdated(configuration);
        } catch (Exception e) {
            logger.warn("Refresh error: {}", e.getMessage());
            logger.debug("Stack trace", e);
        }
    }

    static <OBJECT, CONFIG> CloseableReference<ObjectConfigurationResolver<OBJECT, CONFIG>> newInstance(Config configuration,
                                                                                                        Function<OBJECT, String> selectorFieldAccessor,
                                                                                                        Function<String, CONFIG> dynamicProxyFactory,
                                                                                                        CONFIG defaultConfig,
                                                                                                        Flux<Long> updateTrigger) {
        PeriodicallyRefreshingObjectConfigurationResolver<OBJECT, CONFIG> resolver = new PeriodicallyRefreshingObjectConfigurationResolver<>(
                configuration, selectorFieldAccessor, dynamicProxyFactory, defaultConfig
        );

        Disposable disposable = updateTrigger
                .retryBackoff(Long.MAX_VALUE, RETRY_INTERVAL)
                .subscribe(tick -> resolver.refresh());

        return CloseableReference.<ObjectConfigurationResolver<OBJECT, CONFIG>>newBuilder()
                .withResource(resolver)
                .withCloseAction(disposable::dispose)
                .withSwallowException(true)
                .build();
    }
}
