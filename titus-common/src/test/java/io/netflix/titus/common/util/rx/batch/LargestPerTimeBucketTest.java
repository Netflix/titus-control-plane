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

package io.netflix.titus.common.util.rx.batch;

import java.time.Instant;
import java.util.Queue;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.common.util.rx.batch.Priority.HIGH;
import static io.netflix.titus.common.util.rx.batch.Priority.LOW;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

public class LargestPerTimeBucketTest {
    private static final long NO_BUCKETS = Long.MAX_VALUE;

    private TestScheduler testScheduler;

    @Before
    public void setUp() {
        testScheduler = Schedulers.test();
    }

    @Test
    public void filterOutBatchesNotQueueingForAMinimumPeriod() {
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final Instant past = now.minus(ofMinutes(10));
        final Stream<Batch<MockUpdate, String>> batches = Stream.of(
                Batch.of("first",
                        // only has recent items
                        new MockUpdate(LOW, now, "first", "sub1", "some"),
                        new MockUpdate(HIGH, now, "first", "sub2", "some"),
                        new MockUpdate(LOW, now, "first", "sub3", "some")
                ),
                Batch.of("second",
                        new MockUpdate(LOW, now, "second", "sub1", "some"),
                        new MockUpdate(LOW, now, "second", "sub2", "some"),
                        new MockUpdate(HIGH, past, "second", "sub3", "some")
                ),
                Batch.of("third",
                        new MockUpdate(LOW, past, "third", "sub1", "someState")
                )
        );

        EmissionStrategy strategy = new LargestPerTimeBucket(300_000 /* 5min */, NO_BUCKETS, testScheduler);
        Queue<Batch<MockUpdate, String>> toEmit = strategy.compute(batches);
        // first was filtered out
        assertThat(toEmit).hasSize(2);
        assertThat(toEmit.poll().getIndex()).isEqualTo("second");
        assertThat(toEmit.poll().getIndex()).isEqualTo("third");
    }

    @Test
    public void batchesWithOlderItemsGoFirst() {
        final long groupInBucketsOfMs = 1;
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final Stream<Batch<MockUpdate, String>> batches = Stream.of(
                Batch.of("first",
                        new MockUpdate(LOW, now, "first", "sub1", "foo"),
                        new MockUpdate(LOW, now.minus(ofSeconds(1)), "first", "sub2", "foo")
                ),
                Batch.of("second",
                        new MockUpdate(HIGH, now.minus(ofSeconds(2)), "second", "sub1", "foo"),
                        new MockUpdate(LOW, now, "second", "sub2", "foo")
                ),
                Batch.of("third",
                        new MockUpdate(LOW, now.minus(ofMillis(2_001)), "third", "sub1", "foo"),
                        new MockUpdate(LOW, now, "third", "sub2", "foo")
                ),
                Batch.of("fourth",
                        new MockUpdate(LOW, now.minus(ofMinutes(1)), "fourth", "sub1", "foo"),
                        new MockUpdate(HIGH, now, "third", "sub2", "foo")
                ),
                Batch.of("fifth",
                        new MockUpdate(LOW, now.minus(ofMillis(1)), "fifth", "sub1", "foo"),
                        new MockUpdate(LOW, now, "third", "sub2", "foo")
                )
        );

        EmissionStrategy strategy = new LargestPerTimeBucket(0, groupInBucketsOfMs, testScheduler);
        Queue<Batch<MockUpdate, String>> toEmit = strategy.compute(batches);
        assertThat(toEmit).hasSize(5);
        assertThat(toEmit.poll().getIndex()).isEqualTo("fourth");
        assertThat(toEmit.poll().getIndex()).isEqualTo("third");
        assertThat(toEmit.poll().getIndex()).isEqualTo("second");
        assertThat(toEmit.poll().getIndex()).isEqualTo("first");
        assertThat(toEmit.poll().getIndex()).isEqualTo("fifth");
    }

    @Test
    public void biggerBatchesInTheSameBucketGoFirst() {
        final long groupInBucketsOfMs = 60_000 /* 1min */;
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final Instant past = now.minus(ofMinutes(10));
        final Stream<Batch<MockUpdate, String>> batches = Stream.of(
                // first and second are in the same minute
                Batch.of("smaller",
                        new MockUpdate(LOW, now, "smaller", "sub1", "foo"),
                        new MockUpdate(LOW, past.minus(ofSeconds(10)), "smaller", "sub2", "foo")
                ),
                Batch.of("bigger",
                        new MockUpdate(LOW, past, "bigger", "sub1", "foo"),
                        new MockUpdate(HIGH, now.minus(ofMinutes(5)), "bigger", "sub2", "foo"),
                        new MockUpdate(LOW, now, "bigger", "sub3", "foo")
                ),
                Batch.of("older",
                        new MockUpdate(LOW, now.minus(ofMinutes(15)), "older", "sub1", "foo")
                )
        );

        EmissionStrategy strategy = new LargestPerTimeBucket(0, groupInBucketsOfMs, testScheduler);
        Queue<Batch<MockUpdate, String>> toEmit = strategy.compute(batches);
        assertThat(toEmit).hasSize(3);
        assertThat(toEmit.poll().getIndex()).isEqualTo("older");
        assertThat(toEmit.poll().getIndex()).isEqualTo("bigger");
        assertThat(toEmit.poll().getIndex()).isEqualTo("smaller");
    }
}