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

package com.netflix.titus.common.framework.fit;

import java.util.Map;

/**
 * Action metadata.
 */
public class FitActionDescriptor {

    private final String kind;
    private final String description;
    private final Map<String, String> configurableProperties;

    public FitActionDescriptor(String kind, String description, Map<String, String> configurableProperties) {
        this.kind = kind;
        this.description = description;
        this.configurableProperties = configurableProperties;
    }

    /**
     * Action type unique identifier.
     */
    public String getKind() {
        return kind;
    }

    /**
     * Human readable action description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Configuration properties accepted by the action.
     */
    public Map<String, String> getConfigurableProperties() {
        return configurableProperties;
    }
}
