/*
 * Copyright 2019 Netflix, Inc.
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * Helper functions working with {@link Page} and {@link Pagination} objects.
 */
public final class PaginationUtil {

    private PaginationUtil() {
    }

    /**
     * Function that maps a cursor value to a position in a sorted list.
     */
    public interface CursorIndexOf<T> extends BiFunction<List<T>, String, Optional<Integer>> {
    }

    /**
     * Cursor-based pagination. When a cursor is present, the requested {@link Page#getPageNumber()} pageNumber} is
     * ignored, and will be replaced in the {@link Pagination#getCurrentPage()} response} by an estimated
     * <tt>pageNumber</tt> that includes the first item after the cursor.
     * <p>
     * If the {@link Page#getCursor() requested cursor} is empty, fallback to classic (pageNumber-based) pagination.
     */
    public static <T> Pair<List<T>, Pagination> takePageWithCursor(Page page,
                                                                   List<T> items,
                                                                   Comparator<T> cursorComparator,
                                                                   CursorIndexOf<T> cursorIndexOf,
                                                                   Function<T, String> cursorFactory) {
        List<T> itemsCopy = new ArrayList<>(items);
        itemsCopy.sort(cursorComparator);

        return takePageWithCursorInternal(page, cursorIndexOf, cursorFactory, itemsCopy);
    }

    public static <T> Pair<List<T>, Pagination> takePageWithCursorAndKeyExtractor(Page page,
                                                                                  List<T> items,
                                                                                  Function<T, PaginableEntityKey<T>> keyExtractor,
                                                                                  CursorIndexOf<T> cursorIndexOf,
                                                                                  Function<T, String> cursorFactory) {
        List<T> itemsCopy = PaginableEntityKey.sort(items, keyExtractor);
        return takePageWithCursorInternal(page, cursorIndexOf, cursorFactory, itemsCopy);
    }

    private static <T> Pair<List<T>, Pagination> takePageWithCursorInternal(Page page,
                                                                            CursorIndexOf<T> cursorIndexOf,
                                                                            Function<T, String> cursorFactory,
                                                                            List<T> sortedItems) {
        if (StringExt.isEmpty(page.getCursor())) {
            return takePageWithoutCursor(page, sortedItems, cursorFactory);
        }

        int offset = cursorIndexOf.apply(sortedItems, page.getCursor())
                .orElseThrow(() -> new IllegalArgumentException("Invalid cursor: " + page.getCursor())) + 1;

        int totalItems = sortedItems.size();
        boolean isEmptyResult = offset >= totalItems;
        boolean hasMore = totalItems > (offset + page.getPageSize());
        int endOffset = Math.min(totalItems, offset + page.getPageSize());
        int cursorPosition = endOffset - 1;
        int numberOfPages = numberOfPages(page, totalItems);
        int pageNumber = Math.min(numberOfPages, offset / page.getPageSize());

        Pagination pagination = new Pagination(
                page.toBuilder().withPageNumber(pageNumber).build(),
                hasMore,
                numberOfPages,
                totalItems,
                totalItems == 0 ? "" : cursorFactory.apply(sortedItems.get(cursorPosition)),
                totalItems == 0 ? 0 : cursorPosition
        );

        List<T> pageItems = isEmptyResult ? Collections.emptyList() : sortedItems.subList(offset, endOffset);
        return Pair.of(pageItems, pagination);
    }

    /**
     * {@link Page#getPageNumber() Number} (index) based pagination.
     * <p>
     * Please consider using {@link PaginationUtil#takePageWithCursor(Page, List, Comparator, CursorIndexOf, Function) cursor-based}
     * pagination instead. This mode will be deprecated soon.
     */
    public static <T> Pair<List<T>, Pagination> takePageWithoutCursor(Page page, List<T> items, Function<T, String> cursorFactory) {
        int totalItems = items.size();
        if (totalItems <= 0 || page.getPageSize() <= 0) {
            return Pair.of(Collections.emptyList(), new Pagination(page, false, 0, 0, "", 0));
        }

        int firstItem = page.getPageNumber() * page.getPageSize();
        int lastItem = Math.min(totalItems, firstItem + page.getPageSize());
        boolean more = totalItems > lastItem;
        int totalPages = numberOfPages(page, totalItems);

        List<T> pageItems = firstItem < lastItem
                ? items.subList(firstItem, lastItem)
                : Collections.emptyList();

        String cursor = pageItems.isEmpty() ? "" : cursorFactory.apply(pageItems.get(pageItems.size() - 1));
        int cursorPosition = pageItems.isEmpty() ? 0 : lastItem - 1;

        return Pair.of(pageItems, new Pagination(page, more, totalPages, totalItems, cursor, cursorPosition));
    }

    public static int numberOfPages(Page page, int totalItems) {
        return (totalItems + page.getPageSize() - 1) / page.getPageSize();
    }
}
