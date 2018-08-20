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

import com.netflix.titus.master.config.MasterConfiguration;

import static org.mockito.Mockito.when;

/**
 * Provides pre-initialized {@link MasterConfiguration} objects.
 */
public class ConfigurationMockSamples {

    public static MasterConfiguration withExecutionEnvironment(MasterConfiguration mock) {
        when(mock.getRegion()).thenReturn("us-east-1");
        return mock;
    }
}
