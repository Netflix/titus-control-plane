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

package com.netflix.titus.common.util.collections.index;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.util.collections.index.SamplePerfItem.SlotKey;

import static org.assertj.core.api.Assertions.assertThat;

public class IndexPerf {

    public static final String BY_PRIMARY_KEY = "byPrimaryKey";
    public static final String BY_SLOT = "bySlot";
    private static final String GROUP_BY_SLOT = "groupBySlot";

    public static void main(String[] args) {
        IndexSet<String, SamplePerfItem> indexSet = Indexes.<String, SamplePerfItem>newBuilder()
                .withIndex(BY_PRIMARY_KEY, IndexSpec.<String, String, SamplePerfItem, SamplePerfItem>newBuilder()
                        .withIndexKeyExtractor(SamplePerfItem::getPrimaryKey)
                        .withPrimaryKeyExtractor(SamplePerfItem::getPrimaryKey)
                        .withIndexKeyComparator(String::compareTo)
                        .withPrimaryKeyComparator(String::compareTo)
                        .withTransformer(Function.identity())
                        .build()
                )
                .withIndex(BY_SLOT, IndexSpec.<SlotKey, String, SamplePerfItem, SamplePerfItem>newBuilder()
                        .withIndexKeyExtractor(SamplePerfItem.slotKeyExtractor())
                        .withPrimaryKeyExtractor(SamplePerfItem::getPrimaryKey)
                        .withIndexKeyComparator(SlotKey::compareTo)
                        .withPrimaryKeyComparator(String::compareTo)
                        .withTransformer(Function.identity())
                        .build()
                )
                .withGroup(GROUP_BY_SLOT, IndexSpec.<String, String, SamplePerfItem, SamplePerfItem>newBuilder()
                        .withIndexKeyExtractor(SamplePerfItem::getSlot)
                        .withPrimaryKeyExtractor(SamplePerfItem::getPrimaryKey)
                        .withIndexKeyComparator(String::compareTo)
                        .withPrimaryKeyComparator(String::compareTo)
                        .withTransformer(Function.identity())
                        .build())
                .build();

        Stopwatch stopwatch = Stopwatch.createStarted();
        for (int i = 0; i < 2000; i++) {
            indexSet = indexSet.add(SamplePerfItem.someRandom(20));
        }
        printStats("Initialization", indexSet, stopwatch);

        stopwatch.reset().start();
        List<SamplePerfItem> all = indexSet.<SamplePerfItem>getIndex(BY_PRIMARY_KEY).orderedList();
        for (SamplePerfItem sampleValue : all) {
            indexSet = indexSet.add(Collections.singletonList(sampleValue.nextVersion()));
        }
        printStats("Update", indexSet, stopwatch);

        List<SamplePerfItem> all2 = indexSet.<SamplePerfItem>getIndex(BY_PRIMARY_KEY).orderedList();
        for (int i = 0; i < all.size(); i++) {
            SamplePerfItem before = all.get(i);
            SamplePerfItem after = all2.get(i);
            assertThat(before.getPrimaryKey()).isEqualTo(after.getPrimaryKey());
            assertThat(before.getVersion()).isLessThan(after.getVersion());
        }
    }

    private static void printStats(String header, IndexSet<String, SamplePerfItem> indexSet, Stopwatch stopwatch) {
        System.out.println(header);
        System.out.printf("Elapsed          : %dms\n", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        System.out.println("byPrimaryKey size: " + indexSet.getIndex(BY_PRIMARY_KEY).orderedList().size());
        System.out.println("bySlot size      : " + indexSet.getIndex(BY_SLOT).orderedList().size());
        System.out.println("groupBySlot size : " + indexSet.getGroup(GROUP_BY_SLOT).get().size());
        System.out.println();
    }
}