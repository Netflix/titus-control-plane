/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.model;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import io.netflix.titus.common.util.tuple.Pair;

/**
 * Helper functions working with {@link Page} and {@link Pagination} objects.
 */
public final class PaginationUtil {

    private PaginationUtil() {
    }

    public static <T> Pair<List<T>, Pagination> takePage(Page page, List<T> items, Function<T, String> cursorFactory) {
        int totalItems = items.size();
        if (totalItems <= 0) {
            Pair.of(Collections.emptyList(), new Pagination(page, false, 0, 0, ""));
        }

        int firstItem = page.getPageNumber() * page.getPageSize();
        int lastItem = Math.min(totalItems, firstItem + page.getPageSize());
        boolean more = totalItems > lastItem;
        int totalPages = numberOfPages(page, totalItems);

        List<T> pageItems = firstItem < lastItem
                ? items.subList(firstItem, lastItem)
                : Collections.emptyList();

        String cursor = pageItems.isEmpty() ? "" : cursorFactory.apply(pageItems.get(pageItems.size() - 1));

        return Pair.of(pageItems, new Pagination(page, more, totalPages, totalItems, cursor));
    }

    public static int numberOfPages(Page page, int totalItems) {
        return (totalItems + page.getPageSize() - 1) / page.getPageSize();
    }
}
