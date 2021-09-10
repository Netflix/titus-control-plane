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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class SamplePerfItem {

    private static final AtomicLong id = new AtomicLong();

    private final String primaryKey;
    private final String slot;
    private final long version;

    SamplePerfItem(String primaryKey, String slot, long version) {
        this.primaryKey = primaryKey;
        this.slot = slot;
        this.version = version;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public String getSlot() {
        return slot;
    }

    public long getVersion() {
        return version;
    }

    SamplePerfItem nextVersion() {
        return new SamplePerfItem(primaryKey, slot, version + 1);
    }

    public static SamplePerfItem oneRandom() {
        String primaryKey = "id#" + id.getAndIncrement();
        String slot = "slot#" + (int) (Math.random() * 100);
        return new SamplePerfItem(primaryKey, slot, 0);
    }

    public static List<SamplePerfItem> someRandom(int count) {
        List<SamplePerfItem> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(oneRandom());
        }
        return result;
    }

    static Function<SamplePerfItem, SlotKey> slotKeyExtractor() {
        return SlotKey::new;
    }

    static class SlotKey implements Comparable<SlotKey> {
        private final String slot;
        private final String primaryKey;

        SlotKey(SamplePerfItem sample) {
            this.slot = sample.slot;
            this.primaryKey = sample.primaryKey;
        }

        public String getSlot() {
            return slot;
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        @Override
        public int compareTo(SlotKey other) {
            int cmp = slot.compareTo(other.slot);
            if (cmp != 0) {
                return cmp;
            }
            return primaryKey.compareTo(other.primaryKey);
        }
    }
}
