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

package com.netflix.titus.common.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * System/process helper methods.
 */
public class SystemExt {

    /**
     * Convert {@link System#getProperties()} to a map of strings.
     */
    public static Map<String, String> getSystemPropertyMap() {
        Map<String, String> properties = new HashMap<>();

        System.getProperties().forEach((key, value) -> {
            // Build the string representation of key
            String keyStr = key == null ? null : key.toString();
            if (keyStr == null) {
                return;
            }

            // Build the string representation of value
            String valueStr = value == null ? null : value.toString();

            properties.put(keyStr, valueStr);
        });

        return Collections.unmodifiableMap(properties);
    }

    /**
     * Terminate the JVM process.
     */
    public static void forcedProcessExit(int status) {
        System.err.println("Forced exit: statusCode=" + status);
        new Exception().printStackTrace(System.err);
        System.err.flush();

        Runtime.getRuntime().halt(status);
    }
}
