/*
 * Copyright 2019 Netflix, Inc.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.MapConfig;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.closeable.CloseableReference;
import org.springframework.core.env.Environment;
import reactor.core.publisher.Flux;

public final class Archaius2Ext {

    private static final Config EMPTY_CONFIG = new MapConfig(Collections.emptyMap());

    private static final ConfigProxyFactory DEFAULT_CONFIG_PROXY_FACTORY = new ConfigProxyFactory(
            EMPTY_CONFIG,
            EMPTY_CONFIG.getDecoder(),
            DefaultPropertyFactory.from(EMPTY_CONFIG)
    );

    /**
     * Parse string value like "1, 10, 1000" to an array of {@link Duration} values, assuming the values represent
     * milliseconds.
     */
    public static Supplier<List<Duration>> asDurationList(Supplier<String> configSupplier) {
        Function<String, List<Duration>> memoizedFunction = Evaluators.memoizeLast(v -> StringExt.parseDurationMsList(v).orElse(Collections.emptyList()));
        return () -> memoizedFunction.apply(configSupplier.get());
    }

    /**
     * Observe property value changes. Emits current value on subscriptions.
     */
    public static <T> Flux<T> watch(Property<T> property) {
        Flux<T> observer = Flux.create(emitter -> {
            Property.Subscription subscription = property.subscribe(emitter::next);
            emitter.onCancel(subscription::unsubscribe);
        });

        return Flux.just(property.get()).concatWith(observer);
    }

    /**
     * See {@link #watch(Property)}.
     */
    public static <T> Flux<T> watch(PropertyRepository propertyRepository, String key, Class<T> type) {
        return watch(propertyRepository.get(key, type));
    }

    /**
     * Create Archaius based configuration object initialized with default values. Defaults can be overridden
     * by providing key/value pairs as parameters.
     */
    public static <C> C newConfiguration(Class<C> configType, String... keyValuePairs) {
        if (keyValuePairs.length == 0) {
            return DEFAULT_CONFIG_PROXY_FACTORY.newProxy(configType);
        }

        Preconditions.checkArgument(keyValuePairs.length % 2 == 0, "Expected even number of arguments");

        Map<String, String> props = new HashMap<>();
        int len = keyValuePairs.length / 2;
        for (int i = 0; i < len; i++) {
            props.put(keyValuePairs[i * 2], keyValuePairs[i * 2 + 1]);
        }
        Config config = new MapConfig(props);
        return newConfiguration(configType, config);
    }

    /**
     * Create Archaius based configuration object initialized with default values. Overrides can be provided
     * via the properties parameter.
     */
    public static <C> C newConfiguration(Class<C> configType, Map<String, String> properties) {
        return newConfiguration(configType, new MapConfig(properties));
    }

    /**
     * Create Archaius based configuration object based by the given {@link Config}.
     */
    public static <C> C newConfiguration(Class<C> configType, Config config) {
        ConfigProxyFactory factory = new ConfigProxyFactory(config, config.getDecoder(), DefaultPropertyFactory.from(config));
        return factory.newProxy(configType);
    }

    /**
     * Create Archaius based configuration object based by the given {@link Config}.
     */
    public static <C> C newConfiguration(Class<C> configType, String prefix, Config config) {
        ConfigProxyFactory factory = new ConfigProxyFactory(config, config.getDecoder(), DefaultPropertyFactory.from(config));
        return factory.newProxy(configType, prefix);
    }

    public static <C> C newConfiguration(Class<C> configType, Environment environment, long refreshIntervalMs) {
        return SpringProxyInvocationHandler.newProxy(configType, null, environment, refreshIntervalMs);
    }

    public static <C> C newConfiguration(Class<C> configType, Environment environment) {
        return SpringProxyInvocationHandler.newProxy(configType, null, environment);
    }

    /**
     * Archaius2 dynamic proxy for Spring environment. This implementation supports only features used in Titus.
     * To check what is, and what is not supported please check SpringProxyInvocationHandlerTest.
     */
    public static <C> C newConfiguration(Class<C> configType, String prefix, Environment environment, long refreshIntervalMs) {
        return SpringProxyInvocationHandler.newProxy(configType, prefix, environment, refreshIntervalMs);
    }

    public static <C> C newConfiguration(Class<C> configType, String prefix, Environment environment) {
        return SpringProxyInvocationHandler.newProxy(configType, prefix, environment);
    }

    /**
     * Given object, return configuration associated with it. For properties:
     * a.pattern=a.*
     * a.intValue=123
     * b.pattern=b.*
     * b.intValue=345
     * c.pattern=.*
     * c.intValue=0
     * <p>
     * Evaluate patterns in the alphabetic order until first matches, and map the property subtree onto configuration
     * interface. If nothing matches, returns the default value.
     */
    public static <OBJECT, CONFIG> ObjectConfigurationResolver<OBJECT, CONFIG> newObjectConfigurationResolver(
            Config configuration,
            Function<OBJECT, String> selectorFieldAccessor,
            Class<CONFIG> configType,
            CONFIG defaultConfig) {
        return new Archaius2ObjectConfigurationResolver<>(
                configuration,
                selectorFieldAccessor,
                configType,
                defaultConfig
        );
    }

    /**
     * See {@link #newObjectConfigurationResolver(Config, Function, Class, Object)}. As Spring environment does not support
     * configuration change callbacks, so we have to make an explicit periodic updates.
     *
     * @param updateTrigger on each emitted item from this {@link Flux} instance, a configuration update is made
     * @return resolver instance with a closable reference. Calling {@link CloseableReference#close()}, terminates
     * configuration update subscription.
     */
    public static <OBJECT, CONFIG> CloseableReference<ObjectConfigurationResolver<OBJECT, CONFIG>> newObjectConfigurationResolver(
            String prefix,
            Environment environment,
            Function<OBJECT, String> selectorFieldAccessor,
            Class<CONFIG> configType,
            CONFIG defaultConfig,
            Flux<Long> updateTrigger) {
        String formattedPrefix = SpringConfig.formatPrefix(prefix);
        return PeriodicallyRefreshingObjectConfigurationResolver.newInstance(
                new SpringConfig(formattedPrefix, environment),
                selectorFieldAccessor,
                root -> newConfiguration(configType, formattedPrefix + root, environment),
                defaultConfig,
                updateTrigger
        );
    }

    /**
     * Write {@link Config} to {@link String}.
     */
    public static String toString(Config config) {
        StringBuilder sb = new StringBuilder("{");
        for (Iterator<String> it = config.getKeys(); it.hasNext(); ) {
            String key = it.next();
            sb.append(key).append('=').append(config.getString(key, ""));
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        return sb.append('}').toString();
    }
}
