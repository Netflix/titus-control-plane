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
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;

public class PaginationEvaluator<T> {

    private static final Pattern CURSOR_RE = Pattern.compile("(.*)@(\\d+)");

    private final Function<T, String> idExtractor;
    private final Function<T, Long> timestampExtractor;

    private final Comparator<T> dataComparator;

    public PaginationEvaluator(Function<T, String> idExtractor, Function<T, Long> timestampExtractor) {
        this.idExtractor = idExtractor;
        this.timestampExtractor = timestampExtractor;

        this.dataComparator = (first, second) -> {
            long firstTimestamp = Evaluators.getOrDefault(timestampExtractor.apply(first), 0L);
            long secondTimestamp = Evaluators.getOrDefault(timestampExtractor.apply(second), 0L);
            int timestampCmp = Long.compare(firstTimestamp, secondTimestamp);
            if (timestampCmp != 0) {
                return timestampCmp;
            }

            String firstId = idExtractor.apply(first);
            String secondId = idExtractor.apply(second);
            return firstId.compareTo(secondId);
        };
    }

    public PageResult<T> takePage(Page page, List<T> items) {
        if (page.getPageSize() < 0) {
            throw PaginationException.badPageSize(page.getPageSize());
        }

        if (page.getPageSize() == 0) {
            return takeEmptyPage(page, items);
        }

        List<T> itemsCopy = new ArrayList<>(items);
        itemsCopy.sort(dataComparator);

        if (StringExt.isEmpty(page.getCursor())) {
            return takePageWithoutCursor(page, itemsCopy);
        }

        Pair<String, Long> decodedCursor = decode(page.getCursor());
        return takePageWithCursor(page, itemsCopy, decodedCursor.getLeft(), decodedCursor.getRight());
    }

    private PageResult<T> takeEmptyPage(Page page, List<T> items) {
        return PageResult.pageOf(
                Collections.emptyList(),
                Pagination.newBuilder()
                        .withCurrentPage(page)
                        .withCursor("")
                        .withTotalItems(items.size())
                        .withTotalPages(1)
                        .withHasMore(false)
                        .build()
        );
    }

    /**
     * {@link Page#getPageNumber() Number} (index) based pagination.
     * <p>
     * Please consider using {@link PaginationUtil#takePageWithCursor(Page, List, Comparator, PaginationUtil.CursorIndexOf, Function) cursor-based}
     * pagination instead. This mode will be deprecated soon.
     */
    private PageResult<T> takePageWithoutCursor(Page page, List<T> items) {
        int totalItems = items.size();
        if (totalItems <= 0 || page.getPageSize() <= 0) {
            return PageResult.pageOf(Collections.emptyList(), new Pagination(page, false, 0, 0, "", 0));
        }

        int firstItem = page.getPageNumber() * page.getPageSize();
        int lastItem = Math.min(totalItems, firstItem + page.getPageSize());
        boolean more = totalItems > lastItem;
        int totalPages = numberOfPages(page, totalItems);

        List<T> pageItems = firstItem < lastItem
                ? items.subList(firstItem, lastItem)
                : Collections.emptyList();

        int cursorPosition = pageItems.isEmpty() ? 0 : lastItem - 1;

        return PageResult.pageOf(pageItems, new Pagination(page, more, totalPages, totalItems, encode(pageItems.get(cursorPosition)), cursorPosition));
    }

    private PageResult<T> takePageWithCursor(Page page, List<T> itemsCopy, String cursorId, long cursorTimestamp) {
        int offset = getOffset(itemsCopy, cursorId, cursorTimestamp);

        int totalItems = itemsCopy.size();
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
                totalItems == 0 ? "" : encode(itemsCopy.get(cursorPosition)),
                totalItems == 0 ? 0 : cursorPosition
        );

        List<T> pageItems = isEmptyResult ? Collections.emptyList() : itemsCopy.subList(offset, endOffset);
        return PageResult.pageOf(pageItems, pagination);
    }

    /**
     * Offset is a position of an element after the one pointed by the cursor. If cursor does not point to any
     * existing element, the offset points to the smallest element larger than cursor.
     */
    private int getOffset(List<T> itemsCopy, String cursorId, long cursorTimestamp) {
        if (itemsCopy.isEmpty()) {
            return 0;
        }

        Function<T, Integer> comparator = item -> {
            long itemTimestamp = Evaluators.getOrDefault(timestampExtractor.apply(item), 0L);
            int timestampCmp = Long.compare(cursorTimestamp, itemTimestamp);
            if (timestampCmp != 0) {
                return timestampCmp;
            }

            String itemId = idExtractor.apply(item);
            return cursorId.compareTo(itemId);
        };

        int pos = CollectionsExt.binarySearchLeftMost(itemsCopy, comparator);
        return pos >= 0 ? (pos + 1) : -(pos + 1);
    }

    @VisibleForTesting
    String encode(T value) {
        String id = idExtractor.apply(value);
        long timeStamp = Evaluators.getOrDefault(timestampExtractor.apply(value), 0L);
        return Base64.getEncoder().encodeToString((id + '@' + timeStamp).getBytes());
    }

    private static Pair<String, Long> decode(String encoded) {
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(encoded.getBytes()));
        } catch (Exception e) {
            throw PaginationException.badCursor(encoded);
        }

        Matcher matcher = CURSOR_RE.matcher(decoded);

        boolean matches = matcher.matches();
        if (!matches) {
            throw PaginationException.badCursor(encoded, decoded);
        }

        String jobId = matcher.group(1);
        long timestamp;
        try {
            timestamp = Long.parseLong(matcher.group(2));
        } catch (NumberFormatException e) {
            throw PaginationException.badCursor(encoded, decoded);
        }

        return Pair.of(jobId, timestamp);
    }

    private static int numberOfPages(Page page, int totalItems) {
        return (totalItems + page.getPageSize() - 1) / page.getPageSize();
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<T> {

        private Function<T, String> idExtractor;
        private Function<T, Long> timestampExtractor;

        private Builder() {
        }

        public Builder<T> withIdExtractor(Function<T, String> idExtractor) {
            this.idExtractor = idExtractor;
            return this;
        }

        public Builder<T> withTimestampExtractor(Function<T, Long> timestampExtractor) {
            this.timestampExtractor = timestampExtractor;
            return this;
        }

        public PaginationEvaluator<T> build() {
            return new PaginationEvaluator<>(idExtractor, timestampExtractor);
        }
    }
}
