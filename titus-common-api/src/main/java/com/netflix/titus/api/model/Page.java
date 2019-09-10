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

import java.util.Objects;
import javax.validation.constraints.Min;

import com.netflix.titus.common.model.sanitizer.ClassFieldsNotNull;

/**
 * An entity representing single page of a collection.
 */
@ClassFieldsNotNull
public class Page {

    private static final Page EMPTY = new Page(0, 0, "");
    private static final Page UNLIMITED = new Page(0, Integer.MAX_VALUE / 2, ""); // Do not use MAX_VALUE to prevent overflow

    /**
     * Requested page number, starting from 0 (defaults to 0 if not specified).
     */
    @Min(value = 0, message = "Page number cannot be negative")
    private final int pageNumber;

    /**
     * Requested page size (if not specified, default size is operation specific).
     */
    @Min(value = 0, message = "Page size cannot be negative")
    private final int pageSize;

    private final String cursor;

    public Page(int pageNumber, int pageSize, String cursor) {
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.cursor = cursor;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getPageSize() {
        return pageSize;
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
        Page page = (Page) o;
        return pageNumber == page.pageNumber &&
                pageSize == page.pageSize &&
                Objects.equals(cursor, page.cursor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pageNumber, pageSize, cursor);
    }

    @Override
    public String toString() {
        return "Page{" +
                "pageNumber=" + pageNumber +
                ", pageSize=" + pageSize +
                ", cursor='" + cursor + '\'' +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder(this);
    }

    public static Page empty() {
        return EMPTY;
    }

    public static Page unlimited() {
        return UNLIMITED;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Page source) {
        return new Builder()
                .withPageNumber(source.getPageNumber())
                .withPageSize(source.getPageSize())
                .withCursor(source.getCursor());
    }

    public static final class Builder {
        private int pageNumber;
        private int pageSize;
        private String cursor = "";

        private Builder() {
        }

        public Builder withPageNumber(int pageNumber) {
            this.pageNumber = pageNumber;
            return this;
        }

        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder withCursor(String cursor) {
            this.cursor = cursor;
            return this;
        }

        public Builder but() {
            return newBuilder().withPageNumber(pageNumber).withPageSize(pageSize).withCursor(cursor);
        }

        public Page build() {
            return new Page(pageNumber, pageSize, cursor);
        }
    }
}
