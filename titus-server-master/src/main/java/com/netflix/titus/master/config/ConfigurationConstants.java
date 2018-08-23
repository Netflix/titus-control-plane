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

package com.netflix.titus.master.config;

/**
 * Compile time constants used by configuration interfaces.
 */
public final class ConfigurationConstants {

    public static final String ONE_HOUR = "" + (60 * 60 * 1000);
    public static final String ONE_MINUTE = "" + (60 * 1000);
    public static final String TEN_MINUTES = "" + (10 * 60 * 1000);

    private ConfigurationConstants() {
    }
}
