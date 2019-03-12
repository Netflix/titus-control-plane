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

package com.netflix.titus.runtime.util;

import org.springframework.core.env.Environment;

public class SpringConfigurationUtil {

    public static int getInt(Environment environment, String key, int defaultValue) {
        long value = getLong(environment, key, defaultValue);
        int intValue = (int) value;
        if (intValue != value) {
            return defaultValue;
        }
        return intValue;
    }

    public static long getLong(Environment environment, String key, long defaultValue) {
        String value = environment.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public static String getString(Environment environment, String key, String defaultValue) {
        return environment.getProperty(key, defaultValue);
    }

    public static boolean getBoolean(Environment environment, String key, boolean defaultValue) {
        return environment.getProperty(key, Boolean.class, defaultValue);
    }
}
