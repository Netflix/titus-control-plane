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

package com.netflix.titus.common.util.collections.index;

import java.util.ArrayList;
import java.util.List;

class SampleItem {
    private final String key;
    private final String value;

    public SampleItem(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    static List<SampleItem> newItems(String... keyValuePairs) {
        List<SampleItem> result = new ArrayList<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            result.add(new SampleItem(keyValuePairs[i], keyValuePairs[i + 1]));
        }
        return result;
    }
}
