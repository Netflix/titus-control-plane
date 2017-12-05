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

package io.netflix.titus.common.util.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multiset;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

public class ConcurrentHashMultiMapTest {

    private static final int MINIMUM_THREADS = 10;
    private static final int MAXIMUM_THREADS = 20;
    private static final int MAX_KEYS_PER_THREAD = 1024;
    private static final int MAX_ENTRIES_PER_KEY_PER_THREAD = 8192;
    private static final int POSSIBLE_RANDOM_IDS = 128;
    private static final int POSSIBLE_RANDOM_VALUES = 10;

    private ConcurrentHashMultiMap<String, TestEntity> multiMap;

    @Before
    public void setUp() throws Exception {
        multiMap = new ConcurrentHashMultiMap<>(TestEntity.idExtractor, TestEntity.lastWins);
    }

    @Test
    public void customizableConflictResolution() throws Exception {
        final ConcurrentHashMultiMap.ConflictResolver<TestEntity> onlyReplaceV1 =
                (old, replacement) -> old.value.contains("v1");
        ConcurrentHashMultiMap<String, TestEntity> multiMapThatOnlyReplacesV1 =
                new ConcurrentHashMultiMap<>(TestEntity.idExtractor, onlyReplaceV1);

        multiMapThatOnlyReplacesV1.put("first", new TestEntity("1", "v1"));
        assertThat(multiMapThatOnlyReplacesV1.put("first", new TestEntity("1", "v2"))).isTrue();
        assertThat(multiMapThatOnlyReplacesV1.put("first", new TestEntity("1", "v1"))).isFalse();
    }

    @Test
    public void asMap() throws Exception {
        multiMap.put("firstBucket", new TestEntity("1", "v1"));
        multiMap.put("secondBucket", new TestEntity("2", "v2"));
        multiMap.put("secondBucket", new TestEntity("3", "v3"));
        final Map<String, Collection<TestEntity>> map = multiMap.asMap();
        assertThat(map).containsOnlyKeys("firstBucket", "secondBucket");
        assertThat(map.get("firstBucket")).hasSize(1)
                .contains(new TestEntity("1", "v1"));
        assertThat(map.get("secondBucket")).hasSize(2).containsExactly(
                new TestEntity("2", "v2"),
                new TestEntity("3", "v3")
        );
    }

    @Test
    public void putAndGetReturnsImmutable() throws Exception {
        multiMap.put("first", new TestEntity("1", "v1"));
        multiMap.put("first", new TestEntity("2", "v2"));
        assertThat(multiMap.isEmpty()).isFalse();
        assertThat(multiMap.get("first")).hasSize(2).containsExactly(
                new TestEntity("1", "v1"),
                new TestEntity("2", "v2")
        );
        Throwable thrown = catchThrowable(() -> multiMap.get("first").add(new TestEntity("3", "v3")));
        assertThat(thrown).isInstanceOf(UnsupportedOperationException.class);

    }

    @Test
    public void getNullKey() throws Exception {
        multiMap.put("first", new TestEntity("1", "v1"));
        assertThat(multiMap.get(null)).isEmpty();
    }

    @Test
    public void remove() throws Exception {
        multiMap.put("first", new TestEntity("1", "v1"));
        multiMap.put("first", new TestEntity("2", "v2"));

        assertThat(multiMap.remove(null, null)).isFalse();
        assertThat(multiMap.remove("first", null)).isFalse();
        assertThat(multiMap.remove(null, new TestEntity("foo", "bar"))).isFalse();

        assertThat(multiMap.remove("first", new TestEntity("foo", "bar"))).isFalse();
        assertThat(multiMap.remove("first", new TestEntity("v1", "bar"))).isFalse();
        assertThat(multiMap.get("first")).hasSize(2);

        assertThat(multiMap.remove("first", new TestEntity("1", "v1"))).isTrue();
        assertThat(multiMap.get("first")).hasSize(1)
                .doesNotContain(new TestEntity("1", "v1"));
    }

    @Test
    public void removeIf() throws Exception {
        multiMap.put("first", new TestEntity("1", "v1"));
        multiMap.put("first", new TestEntity("2", "v2"));

        assertThat(multiMap.removeIf(
                "first", new TestEntity("1", "foo"),
                existing -> "foo".equals(existing.value))
        ).isFalse();
        assertThat(multiMap.get("first")).hasSize(2);

        assertThat(multiMap.removeIf(
                "first", new TestEntity("1", "foo"),
                existing -> "v1".equals(existing.value))
        ).isTrue();
        assertThat(multiMap.get("first")).hasSize(1)
                .doesNotContain(new TestEntity("1", "v1"));
    }

    @Test
    public void removeClearsEmptyKeys() throws Exception {
        multiMap.put("first", new TestEntity("1", "v1"));
        multiMap.put("first", new TestEntity("2", "v2"));

        assertThat(multiMap.remove("first", new TestEntity("1", "v1"))).isTrue();
        assertThat(multiMap.get("first")).hasSize(1);
        assertThat(multiMap.remove("first", new TestEntity("2", "v2"))).isTrue();
        assertThat(multiMap.asMap()).doesNotContainKey("first");
        assertThat(multiMap.get("first")).isNull();
    }

    @Test
    public void putAllMultiMap() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        assertThat(multiMap.isEmpty()).isFalse();
        assertThat(multiMap.get("first")).hasSize(1).containsExactly(new TestEntity("1", "v1"));
        assertThat(multiMap.get("second")).hasSize(2).containsExactly(
                new TestEntity("2", "v2"),
                new TestEntity("3", "v3")
        );
    }

    @Test
    public void putAllKey() throws Exception {
        multiMap.putAll("first", ImmutableList.of(
                new TestEntity("1", "v1"),
                new TestEntity("2", "v2")
        ));
        assertThat(multiMap.isEmpty()).isFalse();
        assertThat(multiMap.get("first")).hasSize(2).containsExactly(
                new TestEntity("1", "v1"),
                new TestEntity("2", "v2")
        );
    }

    @Test
    public void replaceValuesIsNotSupported() throws Exception {
        final List<TestEntity> values = ImmutableList.of(new TestEntity("1", "v1"));
        multiMap.putAll("first", values);
        multiMap.replaceValues("first", ImmutableList.of(new TestEntity("1", "v2")));
        assertThat(multiMap.get("first")).hasSize(1)
                .containsExactly(new TestEntity("1", "v2"));
    }

    @Test
    public void removeAll() throws Exception {
        multiMap.putAll("first", ImmutableList.of(
                new TestEntity("1", "v1"),
                new TestEntity("2", "v2")
        ));
        assertThat(multiMap.containsKey("first")).isTrue();
        assertThat(multiMap.removeAll("first")).hasSize(2).containsExactly(
                new TestEntity("1", "v1"),
                new TestEntity("2", "v2")
        );
        assertThat(multiMap.containsKey("first")).isFalse();
    }

    @Test
    public void clear() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        assertThat(multiMap.asMap()).isNotEmpty();
        multiMap.clear();
        assertThat(multiMap.asMap()).isEmpty();
    }

    @Test
    public void keySet() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        assertThat(multiMap.keySet()).containsExactly("first", "second");
    }

    @Test
    public void keys() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        final Multiset<String> keys = multiMap.keys();
        assertThat(keys).hasSize(3);
        assertThat(keys.elementSet()).hasSize(2);
        assertThat(keys.count("first")).isEqualTo(1);
        assertThat(keys.count("second")).isEqualTo(2);
    }

    @Test
    public void values() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        final Collection<TestEntity> values = multiMap.values();
        assertThat(values).hasSize(3);
        assertThat(values).containsExactly(
                new TestEntity("1", "v1"),
                new TestEntity("2", "v2"),
                new TestEntity("3", "v3")
        );
    }

    @Test
    public void entries() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        final Collection<Map.Entry<String, TestEntity>> entries = multiMap.entries();
        assertThat(entries).hasSize(3);
        assertThat(entries).containsExactly(
                SimpleEntry.of("first", new TestEntity("1", "v1")),
                SimpleEntry.of("second", new TestEntity("2", "v2")),
                SimpleEntry.of("second", new TestEntity("3", "v3"))
        );
    }

    @Test
    public void size() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        assertThat(multiMap.size()).isEqualTo(3);
    }

    @Test
    public void containsValue() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        assertThat(multiMap.containsValue(new TestEntity("1", "v1"))).isTrue();
        assertThat(multiMap.containsValue(new TestEntity("2", "v2"))).isTrue();
        assertThat(multiMap.containsValue(new TestEntity("3", "v3"))).isTrue();
    }

    @Test
    public void containsEntry() throws Exception {
        multiMap.putAll(ImmutableListMultimap.of(
                "first", new TestEntity("1", "v1"),
                "second", new TestEntity("2", "v2"),
                "second", new TestEntity("3", "v3")
        ));
        assertThat(multiMap.containsEntry("first", new TestEntity("1", "v1"))).isTrue();
        assertThat(multiMap.containsEntry("first", new TestEntity("2", "v2"))).isFalse();
        assertThat(multiMap.containsEntry("second", new TestEntity("2", "v2"))).isTrue();
        assertThat(multiMap.containsEntry("second", new TestEntity("3", "v3"))).isTrue();
    }

    @Test
    public void concurrentPutAllMultimap() throws Exception {
        ListMultimap<String, TestEntity> allGenerated = randomEntriesInMultipleThreads(itemsPerThread -> {
            multiMap.putAll(itemsPerThread);
        });
        assertThat(multiMap.keySet()).hasSize(allGenerated.keySet().size());
        allGenerated.entries().forEach(
                entry -> assertThat(multiMap.containsEntry(entry.getKey(), entry.getValue())).isTrue()
        );
    }

    @Test
    public void concurrentPutAllPerKey() throws Exception {
        ListMultimap<String, TestEntity> allGenerated = randomEntriesInMultipleThreads(itemsPerThread -> {
            itemsPerThread.asMap().forEach(
                    (key, values) -> multiMap.putAll(key, values)
            );
        });
        assertThat(multiMap.keySet()).hasSize(allGenerated.keySet().size());
        allGenerated.entries().forEach(
                entry -> assertThat(multiMap.containsEntry(entry.getKey(), entry.getValue())).isTrue()
        );
    }

    @Test
    public void concurrentPut() throws Exception {
        ListMultimap<String, TestEntity> allGenerated = randomEntriesInMultipleThreads(itemsPerThread -> {
            itemsPerThread.entries().forEach(
                    entry -> multiMap.put(entry.getKey(), entry.getValue())
            );
        });
        assertThat(multiMap.keySet()).hasSize(allGenerated.keySet().size());
        allGenerated.entries().forEach(
                entry -> assertThat(multiMap.containsEntry(entry.getKey(), entry.getValue())).isTrue()
        );
    }

    private ListMultimap<String, TestEntity> randomEntriesInMultipleThreads(Consumer<ListMultimap<String, TestEntity>> handler)
            throws InterruptedException {
        Random r = new Random();
        final int numberOfThreads = r.nextInt(MAXIMUM_THREADS - MINIMUM_THREADS) + MINIMUM_THREADS;

        final Random random = new Random();
        List<ListMultimap<String, TestEntity>> perThread = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            final ImmutableListMultimap.Builder<String, TestEntity> builder = ImmutableListMultimap.builder();
            for (int key = 0; key < random.nextInt(MAX_KEYS_PER_THREAD); key++) {
                for (int entry = 0; entry < random.nextInt(MAX_ENTRIES_PER_KEY_PER_THREAD); entry++) {
                    String id = "" + random.nextInt(POSSIBLE_RANDOM_IDS);
                    String value = "" + random.nextInt(POSSIBLE_RANDOM_VALUES);
                    builder.put("" + key, new TestEntity(id, value));
                }
            }
            perThread.add(builder.build());
        }
        final ArrayListMultimap<String, TestEntity> flattened = ArrayListMultimap.create();
        perThread.forEach(flattened::putAll);

        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        // add from different threads in parallel
        for (int i = 0; i < numberOfThreads; i++) {
            final ListMultimap<String, TestEntity> currentThreadItems = perThread.get(i);
            new Thread(() -> {
                handler.accept(currentThreadItems);
                latch.countDown();
            }).start();
        }
        assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
        return flattened;
    }

    private static class TestEntity {
        static final ConcurrentHashMultiMap.ValueIdentityExtractor<TestEntity> idExtractor = e -> e.id;
        static final ConcurrentHashMultiMap.ConflictResolver<TestEntity> lastWins = (original, replacement) -> true;

        final String id;
        final String value;

        TestEntity(String id, String value) {
            this.id = id;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestEntity that = (TestEntity) o;

            if (!id.equals(that.id)) {
                return false;
            }
            return value.equals(that.value);
        }
    }
}