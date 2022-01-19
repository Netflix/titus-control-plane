/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.collections.index;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleNestedItem {

    private final String rootId;
    private final Map<String, String> children;

    public SampleNestedItem(String rootId, Map<String, String> children) {
        this.rootId = rootId;
        this.children = children;
    }

    public String getRootId() {
        return rootId;
    }

    public Map<String, String> getChildren() {
        return children;
    }

    public static SampleNestedItem newItem(String rootId, String... keyValuePairs) {
        Map<String, String> children = new HashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            children.put(keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return new SampleNestedItem(rootId, children);
    }

    public static List<SampleNestedItem> newItemList(String rootId, String... keyValuePairs) {
        return Collections.singletonList(newItem(rootId, keyValuePairs));
    }
}
