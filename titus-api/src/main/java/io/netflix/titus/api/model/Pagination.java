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

import java.util.Objects;
import javax.validation.Valid;
import javax.validation.constraints.Min;

import io.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

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

    public Pagination(Page currentPage, boolean hasMore, int totalPages, int totalItems, String cursor) {
        this.currentPage = currentPage;
        this.hasMore = hasMore;
        this.totalPages = totalPages;
        this.totalItems = totalItems;
        this.cursor = cursor;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pagination that = (Pagination) o;
        return hasMore == that.hasMore &&
                totalPages == that.totalPages &&
                totalItems == that.totalItems &&
                Objects.equals(currentPage, that.currentPage) &&
                Objects.equals(cursor, that.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentPage, hasMore, totalPages, totalItems, cursor);
    }

    @Override
    public String toString() {
        return "Pagination{" +
                "currentPage=" + currentPage +
                ", hasMore=" + hasMore +
                ", totalPages=" + totalPages +
                ", totalItems=" + totalItems +
                ", cursor='" + cursor + '\'' +
                '}';
    }
}
