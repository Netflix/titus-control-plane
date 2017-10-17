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

import javax.validation.constraints.Min;

/**
 * An entity representing single page of a collection.
 */
public class Page {

    private static final Page EMPTY = new Page(0, 0);
    private static final Page UNLIMITED = new Page(0, Integer.MAX_VALUE / 2); // Do not use MAX_VALUE to prevent overflow

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

    public Page(int pageNumber, int pageSize) {
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public int getPageSize() {
        return pageSize;
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

        if (pageNumber != page.pageNumber) {
            return false;
        }
        return pageSize == page.pageSize;
    }

    @Override
    public int hashCode() {
        int result = pageNumber;
        result = 31 * result + pageSize;
        return result;
    }

    @Override
    public String toString() {
        return "Page{" +
                "pageNumber=" + pageNumber +
                ", pageSize=" + pageSize +
                '}';
    }

    public static Page empty() {
        return EMPTY;
    }

    public static Page unlimited() {
        return UNLIMITED;
    }
}
