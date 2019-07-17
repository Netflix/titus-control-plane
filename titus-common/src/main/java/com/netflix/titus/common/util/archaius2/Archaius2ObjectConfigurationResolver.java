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

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.netflix.archaius.api.Config;
import com.netflix.archaius.api.ConfigListener;
import com.netflix.titus.common.util.PropertiesExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Archaius2ObjectConfigurationResolver<OBJECT, CONFIG> implements ObjectConfigurationResolver<OBJECT, CONFIG>, ConfigListener, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Archaius2ObjectConfigurationResolver.class);

    private final Config configuration;
    private final Function<OBJECT, String> selectorFieldAccessor;
    private final Class<CONFIG> configType;
    private final CONFIG defaultConfig;

    private volatile SortedMap<String, Rule<CONFIG>> configMap = Collections.emptySortedMap();

    Archaius2ObjectConfigurationResolver(Config configuration,
                                         Function<OBJECT, String> selectorFieldAccessor,
                                         Class<CONFIG> configType,
                                         CONFIG defaultConfig) {
        this.configuration = configuration;
        this.selectorFieldAccessor = selectorFieldAccessor;
        this.configType = configType;
        this.defaultConfig = defaultConfig;

        configuration.addListener(this);
        doUpdate();
    }

    @Override
    public void close() {
        configuration.removeListener(this);
    }

    @Override
    public CONFIG resolve(OBJECT object) {
        if (object == null) {
            return defaultConfig;
        }

        String selectorValue = selectorFieldAccessor.apply(object);
        if (selectorValue == null) {
            return defaultConfig;
        }

        for (Map.Entry<String, Rule<CONFIG>> entry : configMap.entrySet()) {
            Rule<CONFIG> rule = entry.getValue();
            if (rule.getPattern().matcher(selectorValue).matches()) {
                return rule.getConfig();
            }
        }
        return defaultConfig;
    }

    @Override
    public void onConfigAdded(Config config) {
        doUpdate();
    }

    @Override
    public void onConfigRemoved(Config config) {
        doUpdate();
    }

    @Override
    public void onConfigUpdated(Config config) {
        doUpdate();
    }

    @Override
    public void onError(Throwable error, Config config) {
        logger.debug("Configuration error", error);
    }

    private void doUpdate() {
        List<String> keys = Lists.newArrayList(configuration.getKeys());
        Set<String> roots = PropertiesExt.getRootNames(keys, 1);

        SortedMap<String, Rule<CONFIG>> newConfigMap = new TreeMap<>();
        roots.forEach(root -> processSubKeys(root).ifPresent(value -> newConfigMap.put(root, value)));

        this.configMap = newConfigMap;
    }

    private Optional<Rule<CONFIG>> processSubKeys(String root) {
        String patternProperty = root + ".pattern";
        String patternString = configuration.getString(patternProperty, null);
        if (patternString == null) {
            return Optional.empty();
        }

        Rule<CONFIG> previous = configMap.get(root);

        Pattern pattern;
        if (previous != null && previous.getPatternString().equals(patternString)) {
            pattern = previous.getPattern();
        } else {
            try {
                pattern = Pattern.compile(patternString);
            } catch (Exception e) {
                logger.warn("Invalid regular expression in property {}: {}", patternProperty, patternString);
                return Optional.ofNullable(previous);
            }
        }

        if (previous != null) {
            return Optional.of(new Rule<>(patternString, pattern, previous.getConfig()));
        }

        return Optional.of(new Rule<>(patternString, pattern, Archaius2Ext.newConfiguration(configType, root, configuration)));
    }

    private static class Rule<CONFIG> {

        private final String patternString;
        private final Pattern pattern;
        private final CONFIG config;

        private Rule(String patternString, Pattern pattern, CONFIG config) {
            this.patternString = patternString;
            this.pattern = pattern;
            this.config = config;
        }

        private String getPatternString() {
            return patternString;
        }

        private Pattern getPattern() {
            return pattern;
        }

        private CONFIG getConfig() {
            return config;
        }
    }
}
