/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.environment;

/**
 * Specialized version of {@link MyEnvironment} that supports configuration updates. To be used by test code.
 */
public interface MyMutableEnvironment extends MyEnvironment {

    /**
     * Set a new property value, returning the previous value or null if previously not set.
     */
    String setProperty(String key, String value);

    /**
     * Remove a property value, returning the previous value or null if previously not set.
     */
    String removeProperty(String key);
}
