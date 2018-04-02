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

package com.netflix.titus.master;

import java.util.Properties;

import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.config.MapConfig;

/**
 * For non-DI environments.
 */
public class CoreConfigurationFactory {

    public static CoreConfiguration bind(Properties properties) {
        return new ConfigProxyFactory(MapConfig.from(properties)).newProxy(CoreConfiguration.class);
    }
}
