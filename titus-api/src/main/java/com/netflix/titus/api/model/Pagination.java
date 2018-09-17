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

package com.netflix.titus.api.model;

import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.Min;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

/**
 * An entity representing pagination information returned to a client iterating over its elements.
 * It includes current page that the client requested, and the total collection size.
 * As not always pageCount * pageSize == itemCount, the item count is included as well.
 */
@ClassFieldsNotNull
public class Pagination {

    @Valid
    private final Page currentPage;

    private final boolean hasMore;

    @Min(value = 0, message = "'totalPages' value cannot be negative")
    private final int totalPages;

    @Min(value = 0, message = "'totalItems' value cannot be negative")
    private final int totalItems;

    private final String cursor;

    /**
     * Position of the cursor relative to <tt>totalItems</tt>
     */
    private final int cursorPosition;

    public Pagination(Page currentPage, boolean hasMore, int totalPages, int totalItems, String cursor, int cursorPosition) {
        this.currentPage = currentPage;
        this.hasMore = hasMore;
        this.totalPages = totalPages;
        this.totalItems = totalItems;
        this.cursor = cursor;
        this.cursorPosition = cursorPosition;
    }

    public Page getCurrentPage() {
        return currentPage;
    }

    public boolean hasMore() {
        return hasMore;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public int getTotalItems() {
        return totalItems;
    }

    public String getCursor() {
        return cursor;
    }

    public int getCursorPosition() {
        return cursorPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Pagination)) {
            return false;
        }
        Pagination that = (Pagination) o;
        return hasMore == that.hasMore &&
                totalPages == that.totalPages &&
                totalItems == that.totalItems &&
                cursorPosition == that.cursorPosition &&
                Objects.equals(currentPage, that.currentPage) &&
                Objects.equals(cursor, that.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentPage, hasMore, totalPages, totalItems, cursor, cursorPosition);
    }

    @Override
    public String toString() {
        return "Pagination{" +
                "currentPage=" + currentPage +
                ", hasMore=" + hasMore +
                ", totalPages=" + totalPages +
                ", totalItems=" + totalItems +
                ", cursor='" + cursor + '\'' +
                ", cursorPosition=" + cursorPosition +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Pagination empty(Page page) {
        return new Pagination(page, false, 0, 0, "", 0);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Pagination other) {
        return new Builder()
                .withCurrentPage(other.currentPage)
                .withHasMore(other.hasMore)
                .withTotalPages(other.totalPages)
                .withTotalItems(other.totalItems)
                .withCursor(other.cursor)
                .withCursorPosition(other.cursorPosition);
    }

    public static final class Builder {
        private Page currentPage;
        private boolean hasMore;
        private int totalPages;
        private int totalItems;
        private String cursor;
        private int cursorPosition;

        private Builder() {
        }

        public Builder withCurrentPage(Page currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        public Builder withHasMore(boolean hasMore) {
            this.hasMore = hasMore;
            return this;
        }

        public Builder withTotalPages(int totalPages) {
            this.totalPages = totalPages;
            return this;
        }

        public Builder withTotalItems(int totalItems) {
            this.totalItems = totalItems;
            return this;
        }

        public Builder withCursor(String cursor) {
            this.cursor = cursor;
            return this;
        }

        public Builder withCursorPosition(int cursorPosition) {
            this.cursorPosition = cursorPosition;
            return this;
        }

        public Builder but() {
            return newBuilder().withCurrentPage(currentPage).withHasMore(hasMore).withTotalPages(totalPages).withTotalItems(totalItems).withCursor(cursor).withCursorPosition(cursorPosition);
        }

        public Pagination build() {
            return new Pagination(currentPage, hasMore, totalPages, totalItems, cursor, cursorPosition);
        }
    }
}
