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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.netflix.archaius.api.Config;
import com.netflix.archaius.config.AbstractConfig;
import com.netflix.titus.common.util.StringExt;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;

public class SpringConfig extends AbstractConfig {

    private final String prefix;
    private final Environment environment;

    public SpringConfig(String prefix, Environment environment) {
        this.prefix = formatPrefix(prefix);
        this.environment = environment;
    }

    @Override
    public Object getRawProperty(String key) {
        return environment.getProperty(fullKey(key));
    }

    @Override
    public boolean containsKey(String key) {
        return environment.containsProperty(fullKey(key));
    }

    @Override
    public Iterator<String> getKeys() {
        return getAllProperties().keySet().iterator();
    }

    @Override
    public Config getPrefixedView(String prefix) {
        return new SpringConfig(this.prefix + prefix, environment);
    }

    @Override
    public boolean isEmpty() {
        return getAllProperties().isEmpty();
    }

    private String fullKey(String key) {
        return prefix.isEmpty() ? key : prefix + key;
    }

    /**
     * Based on the following recommendation: https://github.com/spring-projects/spring-framework/issues/14874
     */
    private Map<String, Object> getAllProperties() {
        Map<String, Object> all = new HashMap<>();
        if (environment instanceof ConfigurableEnvironment) {
            for (PropertySource<?> propertySource : ((ConfigurableEnvironment) environment).getPropertySources()) {
                if (propertySource instanceof EnumerablePropertySource) {
                    for (String key : ((EnumerablePropertySource) propertySource).getPropertyNames()) {
                        if (key.startsWith(prefix)) {
                            all.put(key.substring(prefix.length()), propertySource.getProperty(key));
                        }
                    }
                }
            }
        }
        return all;
    }

    static String formatPrefix(String prefix) {
        return StringExt.isEmpty(prefix) ? "" : (prefix.endsWith(".") ? prefix : prefix + '.');
    }
}
