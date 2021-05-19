/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.util.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class OneItemIterator<T> implements Iterator<T> {

    private final T item;
    private boolean returned;

    public OneItemIterator(T item) {
        this.item = item;
    }

    @Override
    public boolean hasNext() {
        return !returned;
    }

    @Override
    public T next() {
        if (returned) {
            throw new NoSuchElementException("no more items to return");
        }
        returned = true;
        return item;
    }
}
