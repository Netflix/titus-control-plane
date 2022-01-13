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

import java.util.Map;

/**
 * A map from some unique index key to a value. A given input value may be split into several values with unique ids
 * within the index. This is the opposite of {@link Group} which aggregates input values by some key.
 * For example, a JobWithTasks input value could be mapped to collection of tasks, with the UNIQUE_INDEX_KEY being the task id.
 */
public interface Index<UNIQUE_INDEX_KEY, VALUE> {
    Map<UNIQUE_INDEX_KEY, VALUE> get();
}
