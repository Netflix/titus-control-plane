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

package io.netflix.titus.common.network.http;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

public class Headers {

    Multimap<String, String> delegate;

    public Headers() {
        this.delegate = ArrayListMultimap.create();
    }

    public String get(String name) {
        Collection<String> values = delegate.get(name);
        if (values.size() > 0) {
            Iterables.getLast(values);
        }
        return null;
    }

    public Set<String> names() {
        return delegate.keySet();
    }

    public List<String> values(String name) {
        return ImmutableList.copyOf(delegate.get(name));
    }

    public void put(String name, String value) {
        delegate.put(name, value);
    }

    public void set(String name, String value) {
        set(name, Collections.singletonList(value));
    }

    public void set(String name, List values) {
        delegate.replaceValues(name, values);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Headers headers1 = (Headers) o;

        return delegate != null ? delegate.equals(headers1.delegate) : headers1.delegate == null;
    }

    @Override
    public int hashCode() {
        return delegate != null ? delegate.hashCode() : 0;
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
