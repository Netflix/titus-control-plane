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

import java.util.Comparator;
import java.util.List;

import com.netflix.titus.common.util.CollectionsExt;

class PCollectionUtil {

    /**
     * {@link java.util.Collections#binarySearch(List, Object)} expects the provided collection to implement
     * {@link java.util.RandomAccess} for non-iterator implementation. Otherwise it does linear scan.
     * We reimplement it here for {@link org.pcollections.PVector} which has log(n) time for value retrieval at an index
     * (see https://en.wikipedia.org/wiki/Binary_search_algorithm).
     */
    public static <T> int binarySearch(List<? extends T> list, T key, Comparator<? super T> comparator) {
        if (CollectionsExt.isNullOrEmpty(list)) {
            return -1;
        }

        int left = 0;
        int right = list.size() - 1;

        while (left <= right) {
            int pos = (left + right) / 2;
            T value = list.get(pos);
            int c = comparator.compare(value, key);

            if (c < 0) {
                left = pos + 1;
            } else if (c > 0) {
                right = pos - 1;
            } else {
                return pos;
            }
        }
        return -(left + 1);
    }
}
