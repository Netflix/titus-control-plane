/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.collections.index;

import java.io.Closeable;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.ExceptionExt;
import org.junit.Test;

public class IndexSetMetricsTest {

    private final Registry registry = new DefaultRegistry();

    private static final IndexSpec<Character, String, SampleItem, String> SPEC = IndexSpec.<Character, String, SampleItem, String>newBuilder()
            .withIndexKeyExtractor(item -> item.getKey().charAt(0))
            .withPrimaryKeyExtractor(SampleItem::getKey)
            .withIndexKeyComparator(Character::compareTo)
            .withPrimaryKeyComparator(String::compareTo)
            .withTransformer(SampleItem::getValue)
            .build();

    @Test
    public void testMetrics() {
        IndexSetHolder<String, SampleItem> indexSetHolder = new IndexSetHolderBasic<>(
                Indexes.<String, SampleItem>newBuilder()
                        .withGroup("testGroup", SPEC)
                        .withIndex("testIndex", SPEC)
                        .build()
        );
        Closeable closeable = Indexes.monitor(registry.createId("test"), indexSetHolder, registry);
        ExceptionExt.silent(closeable::close);
    }
}