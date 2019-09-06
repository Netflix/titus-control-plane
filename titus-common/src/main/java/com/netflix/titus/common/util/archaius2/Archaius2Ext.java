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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.Config;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.archaius.config.MapConfig;
import reactor.core.publisher.Flux;

public final class Archaius2Ext {

    private static final Config EMPTY_CONFIG = new MapConfig(Collections.emptyMap());

    private static final ConfigProxyFactory DEFAULT_CONFIG_PROXY_FACTORY = new ConfigProxyFactory(
            EMPTY_CONFIG,
            EMPTY_CONFIG.getDecoder(),
            DefaultPropertyFactory.from(EMPTY_CONFIG)
    );

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
}
