/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.api.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

class PaginableItems {

    static final long TIME_STEP_MS = 100;

    static final PaginationEvaluator<PaginableItem> PAGINATION_EVALUATOR = PaginationEvaluator.<PaginableItem>newBuilder()
            .withIdExtractor(PaginableItem::getId)
            .withTimestampExtractor(PaginableItem::getTimestamp)
            .build();

    static List<PaginableItem> items(int count) {
        List<PaginableItem> result = new ArrayList<>(count);
        long timestamp = 0;
        for (int i = 0; i < count; i++) {
            result.add(new PaginableItem(UUID.randomUUID().toString(), timestamp));
            timestamp += TIME_STEP_MS;
        }
        return result;
    }

    static Set<String> idsOf(List<PaginableItem> items) {
        return items.stream().map(PaginableItem::getId).collect(Collectors.toSet());
    }
}
