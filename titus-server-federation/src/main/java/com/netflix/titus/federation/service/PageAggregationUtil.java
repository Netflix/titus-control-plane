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

package com.netflix.titus.federation.service;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.common.util.tuple.Pair;

final class PageAggregationUtil {
    private PageAggregationUtil() {
    }

    static <T> Pair<List<T>, Pagination> takeCombinedPage(Page requested,
                                                          List<T> combinedItems,
                                                          Pagination combinedPagination,
                                                          Function<T, T> stackNameDecorator,
                                                          Comparator<T> cursorComparator,
                                                          Function<T, String> cursorFactory) {
        List<T> sorted = combinedItems.stream()
                .map(stackNameDecorator)
                .sorted(cursorComparator)
                .collect(Collectors.toList());

        int lastItemOffset = Math.min(sorted.size(), requested.getPageSize());
        List<T> pageItems = sorted.subList(0, lastItemOffset);
        String cursor = sorted.isEmpty() ? "" : cursorFactory.apply(pageItems.get(pageItems.size() - 1));

        // first item position relative to totalItems from all Cells
        int firstItemPosition = Math.max(0, combinedPagination.getCursorPosition() - (sorted.size() - 1));
        int pageNumber = firstItemPosition / requested.getPageSize();
        Pagination finalPagination = Pagination.newBuilder(combinedPagination)
                .setCurrentPage(Page.newBuilder(requested).setPageNumber(pageNumber))
                .setCursor(cursor)
                .setCursorPosition(firstItemPosition + lastItemOffset - 1)
                .setHasMore(combinedPagination.getHasMore() || lastItemOffset < sorted.size())
                .build();

        return Pair.of(pageItems, finalPagination);
    }

    static Pagination combinePagination(Pagination one, Pagination other) {
        int cursorPosition = one.getCursorPosition() + other.getCursorPosition();
        if (one.getTotalItems() > 0 && other.getTotalItems() > 0) {
            // the cursorPosition on each cell always points to (totalItemsReturned - 1), when merging two cells
            // with items, we need to compensate two deductions on the total number of items, so the final (merged)
            // cursorPosition is still (totalItemsBeingReturned - 1).
            // Note that when either one of the cells is empty, the cursorPosition from the empty cell will be 0 and
            // there is nothing to compensate since there are no items in that cell.
            cursorPosition++;
        }
        return Pagination.newBuilder()
                // combined currentPage.pageNumber and cursor will be computed later
                .setHasMore(one.getHasMore() || other.getHasMore())
                .setTotalPages(one.getTotalPages() + other.getTotalPages())
                .setTotalItems(one.getTotalItems() + other.getTotalItems())
                .setCursorPosition(cursorPosition)
                .build();
    }
}
