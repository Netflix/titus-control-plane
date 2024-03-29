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

import java.util.List;

/**
 * Ordered list of values. The ordering is set by a pair of an arbitrary index key and the primary key.
 * {@link Order} object is immutable. Changes to an order produce new copies of it.
 */
public interface Order<VALUE> {

    /**
     * @return immutable ordered set of values.
     */
    List<VALUE> orderedList();
}
