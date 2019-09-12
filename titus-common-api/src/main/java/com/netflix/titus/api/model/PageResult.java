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

import java.util.List;
import java.util.Objects;

public class PageResult<T> {

    private final List<T> items;
    private final Pagination pagination;

    public PageResult(List<T> items, Pagination pagination) {
        this.items = items;
        this.pagination = pagination;
    }

    public List<T> getItems() {
        return items;
    }

    public Pagination getPagination() {
        return pagination;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PageResult<?> that = (PageResult<?>) o;
        return Objects.equals(items, that.items) &&
                Objects.equals(pagination, that.pagination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(items, pagination);
    }

    @Override
    public String toString() {
        return "PageResult{" +
                "items=" + items +
                ", pagination=" + pagination +
                '}';
    }

    public static <T> PageResult<T> pageOf(List<T> items, Pagination pagination) {
        return new PageResult<>(items, pagination);
    }
}
