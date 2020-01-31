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
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import static com.netflix.titus.api.model.PaginableItems.PAGINATION_EVALUATOR;
import static org.assertj.core.api.Assertions.assertThat;

public class PaginationEvaluatorTest {

    private static final Page FIRST_PAGE = Page.newBuilder().withPageSize(10).build();

    @Test
    public void testEmpty() {
        PageResult<PaginableItem> pageResult = PAGINATION_EVALUATOR.takePage(FIRST_PAGE, Collections.emptyList());
        assertThat(pageResult.getItems()).isEmpty();
        assertThat(pageResult.getPagination().hasMore()).isFalse();
    }

    @Test
    public void testEmptyWithCursor() {
        Page page = FIRST_PAGE.toBuilder()
                .withCursor(PAGINATION_EVALUATOR.encode(new PaginableItem("id", 1_000)))
                .build();
        PageResult<PaginableItem> pageResult = PAGINATION_EVALUATOR.takePage(page, Collections.emptyList());
        assertThat(pageResult.getItems()).isEmpty();
        assertThat(pageResult.getPagination().hasMore()).isFalse();
    }

    @Test
    public void testPagination() {
        paginateThroughAllRemainingPages(FIRST_PAGE, PaginableItems.items(100));
    }

    @Test
    public void testPaginationWithPageSizeZero() {
        PageResult<PaginableItem> pageResult = PAGINATION_EVALUATOR.takePage(Page.empty(), PaginableItems.items(2));
        assertThat(pageResult.getPagination().getTotalItems()).isEqualTo(2);
        assertThat(pageResult.getItems()).isEmpty();
    }

    @Test
    public void testPaginationWithCursor() {
        List<PaginableItem> items = PaginableItems.items(100);
        PaginableItem removed = items.remove(50);
        Page page = FIRST_PAGE.toBuilder().withCursor(PAGINATION_EVALUATOR.encode(removed)).build();
        paginateThroughAllRemainingPages(page, items.subList(50, items.size()));
    }

    @Test(expected = PaginationException.class)
    public void testPaginationWithBadPageSize() {
        PAGINATION_EVALUATOR.takePage(FIRST_PAGE.toBuilder().withPageSize(-1).build(), PaginableItems.items(2));
    }

    @Test(expected = PaginationException.class)
    public void testPaginationWithBadCursor() {
        PAGINATION_EVALUATOR.takePage(FIRST_PAGE.toBuilder().withCursor("badCursor").build(), PaginableItems.items(2));
    }

    private List<PaginableItem> paginateThroughAllRemainingPages(Page page, List<PaginableItem> items) {
        List<PaginableItem> fetched = new ArrayList<>();
        boolean hasMore = true;
        while (hasMore) {
            PageResult<PaginableItem> pageResult = PAGINATION_EVALUATOR.takePage(page, items);
            fetched.addAll(pageResult.getItems());

            page = page.toBuilder().withCursor(pageResult.getPagination().getCursor()).build();
            hasMore = pageResult.getPagination().hasMore();
        }

        verifyPaginationResult(items, fetched);

        return fetched;
    }

    private void verifyPaginationResult(List<PaginableItem> items, List<PaginableItem> fetched) {
        assertThat(fetched.size()).isEqualTo(items.size());
        assertThat(PaginableItems.idsOf(fetched)).isEqualTo(PaginableItems.idsOf(items));

        long minTimestamp = items.stream().mapToLong(PaginableItem::getTimestamp).min().orElse(0);
        assertThat(items.get(0).getTimestamp()).isEqualTo(minTimestamp);

        for (int i = 1; i < fetched.size(); i++) {
            assertThat(items.get(i).getTimestamp()).isEqualTo(items.get(i - 1).getTimestamp() + PaginableItems.TIME_STEP_MS);
        }
    }
}