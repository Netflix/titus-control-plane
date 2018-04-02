/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.common.util.rx.batch;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.spectator.api.NoopRegistry;
import com.netflix.titus.common.util.limiter.tokenbucket.RefillStrategy;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.observers.AssertableSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import static com.netflix.titus.common.util.rx.batch.Priority.High;
import static com.netflix.titus.common.util.rx.batch.Priority.Low;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RateLimitedBatcherTest {
    private TestScheduler testScheduler;
    private int minimumTimeInQueueMs;
    private int timeWindowBucketSizeMs;
    private EmissionStrategy strategy;
    private TokenBucket tokenBucket;
    private Random random;

    @Before
    public void setUp() {
        testScheduler = Schedulers.test();
        timeWindowBucketSizeMs = 1_000;
        minimumTimeInQueueMs = 1_000;
        strategy = new LargestPerTimeBucket(minimumTimeInQueueMs, timeWindowBucketSizeMs, testScheduler);
        RefillStrategy refillStrategy = mock(RefillStrategy.class);
        when(refillStrategy.getTimeUntilNextRefill(any())).thenAnswer(
                invocation -> invocation.<TimeUnit>getArgument(0).convert(1_000, TimeUnit.MILLISECONDS)
        );
        tokenBucket = mock(TokenBucket.class);
        when(tokenBucket.getRefillStrategy()).thenReturn(refillStrategy);
        when(tokenBucket.tryTake()).thenReturn(true);
        random = new Random();
    }

    /**
     * 1. older (bucketized) first
     * 2. in each bucket, larger batches first
     */
    @Test
    public void emitAccordingToStrategy() {
        final int initialDelay = minimumTimeInQueueMs;
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final Instant start = now.minus(ofHours(1));
        final Instant firstBucket = start.plus(ofMillis(timeWindowBucketSizeMs));
        final Instant secondBucket = firstBucket.plus(ofMillis(timeWindowBucketSizeMs));
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(initialDelay);

        final List<Batch<BatchableOperationMock, String>> expected = Arrays.asList(
                // older bucket
                Batch.of("resource1",
                        new BatchableOperationMock(Low, start, "resource1", "sub1", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(start), "resource1", "sub2", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(firstBucket), "resource1", "sub3", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(secondBucket), "resource1", "sub4", "create")
                ),
                // larger in firstBucket
                Batch.of("resource2",
                        new BatchableOperationMock(Low, randomWithinBucket(firstBucket), "resource2", "sub1", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(firstBucket), "resource2", "sub2", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(secondBucket), "resource2", "sub3", "create")
                ),
                // larger in secondBucket
                Batch.of("resource3",
                        new BatchableOperationMock(Low, randomWithinBucket(secondBucket), "resource3", "sub1", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(secondBucket), "resource3", "sub2", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(secondBucket), "resource3", "sub3", "create")
                ),
                // last
                Batch.of("resource4",
                        new BatchableOperationMock(Low, randomWithinBucket(secondBucket), "resource4", "sub1", "create"),
                        new BatchableOperationMock(Low, randomWithinBucket(secondBucket), "resource4", "sub2", "create")
                )
        );

        final AssertableSubscriber<Batch<BatchableOperationMock, String>> subscriber = Observable.from(toUpdateList(expected))
                .lift(batcher)
                .test();

        testScheduler.advanceTimeBy(initialDelay, TimeUnit.MILLISECONDS);
        subscriber.assertNoErrors()
                .assertValueCount(expected.size())
                .assertCompleted();

        final List<Batch<BatchableOperationMock, String>> events = subscriber.getOnNextEvents();
        Assertions.assertThat(events).hasSize(expected.size());
        for (int i = 0; i < expected.size(); i++) {
            final Batch<BatchableOperationMock, String> actual = events.get(i);
            final Batch<BatchableOperationMock, String> expectedBatch = expected.get(i);
            final List<BatchableOperationMock> expectedUpdates = expectedBatch.getItems();
            final BatchableOperationMock[] expectedUpdatesArray = expectedUpdates.toArray(new BatchableOperationMock[expectedUpdates.size()]);

            assertThat(actual).isNotNull();
            assertThat(actual.getIndex()).isEqualTo(expectedBatch.getIndex());
            assertThat(actual.getItems()).containsExactlyInAnyOrder(expectedUpdatesArray);
        }
    }

    @Test
    public void exponentialBackoffWhenRateLimited() {
        when(tokenBucket.tryTake()).thenReturn(false);

        final int initialDelayMs = 1_000;
        final int maxDelayMs = 10 * initialDelayMs;
        final List<Integer> delays = Arrays.asList(initialDelayMs, 2 * initialDelayMs, 4 * initialDelayMs, 8 * initialDelayMs, maxDelayMs);
        final long afterAllDelays = delays.stream().mapToLong(Integer::longValue).sum() + 2 * maxDelayMs /* two extra round at the end */;
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(initialDelayMs, maxDelayMs);

        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final BatchableOperationMock update = new BatchableOperationMock(Low, now.minus(ofSeconds(5)), "resource1", "sub1", "create");
        final BatchableOperationMock updateAfterDelays = new BatchableOperationMock(Low, now.plus(ofMillis(afterAllDelays)), "resource2", "sub1", "create");
        final Observable<BatchableOperationMock> updates = Observable.from(Arrays.asList(update, updateAfterDelays));
        final AssertableSubscriber<Batch<BatchableOperationMock, String>> subscriber = updates.lift(batcher).test();

        int attempts = 0;
        for (int delay : delays) {
            testScheduler.advanceTimeBy(delay - 1, TimeUnit.MILLISECONDS);
            verify(tokenBucket, times(attempts)).tryTake();
            subscriber.assertNoTerminalEvent().assertNoValues();
            // after a delay, we try again
            testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
            verify(tokenBucket, times(++attempts)).tryTake();
            subscriber.assertNoTerminalEvent().assertNoValues();
        }

        // capped by maxDelayMs
        testScheduler.advanceTimeBy(maxDelayMs - 1, TimeUnit.MILLISECONDS);
        verify(tokenBucket, times(attempts)).tryTake();
        subscriber.assertNoTerminalEvent().assertNoValues();
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        verify(tokenBucket, times(++attempts)).tryTake();
        subscriber.assertNoTerminalEvent().assertNoValues();

        // stop being rate limited
        when(tokenBucket.tryTake()).thenReturn(true);
        testScheduler.advanceTimeBy(maxDelayMs, TimeUnit.MILLISECONDS);
        verify(tokenBucket, times(++attempts)).tryTake();
        //noinspection unchecked
        subscriber.assertNoErrors()
                .assertValueCount(1)
                .assertValuesAndClear(Batch.of("resource1", update));

        // delay is reset after stopped being rate limited
        testScheduler.advanceTimeBy(initialDelayMs, TimeUnit.MILLISECONDS);
        verify(tokenBucket, times(++attempts)).tryTake();
        //noinspection unchecked
        subscriber.assertNoErrors()
                .assertValueCount(1)
                .assertValuesAndClear(Batch.of("resource2", updateAfterDelays))
                .assertCompleted();
    }

    @Test
    public void pendingItemsAreFlushedAfterUpstreamCompletes() {
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(minimumTimeInQueueMs);

        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final BatchableOperationMock first = new BatchableOperationMock(Low, now.minus(ofSeconds(5)), "resource1", "sub1", "create");
        final BatchableOperationMock second = new BatchableOperationMock(Low, now.minus(ofSeconds(3)), "resource2", "sub1", "create");
        final Observable<BatchableOperationMock> updates = Observable.from(Arrays.asList(first, second));

        final AssertableSubscriber<Batch<BatchableOperationMock, String>> subscriber = updates.lift(batcher).test();
        testScheduler.advanceTimeBy(1, TimeUnit.MINUTES);
        //noinspection unchecked
        subscriber.assertNoErrors()
                .assertValueCount(2)
                .assertValues(
                        Batch.of("resource1", Collections.singletonList(first)),
                        Batch.of("resource2", Collections.singletonList(second))
                )
                .assertCompleted();
    }

    @Test
    public void minimumTimeInQueue() {
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(1);

        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final List<BatchableOperationMock> updatesList = Arrays.asList(
                new BatchableOperationMock(Low, now, "resource1", "sub2", "create"),
                new BatchableOperationMock(Low, now.minus(ofMillis(minimumTimeInQueueMs + 1)), "resource1", "sub1", "create")
        );

        final AssertableSubscriber<Batch<BatchableOperationMock, String>> subscriber = Observable.from(updatesList).lift(batcher).test();
        testScheduler.triggerActions();
        subscriber.assertNoTerminalEvent().assertNoValues();

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        subscriber.assertNoErrors()
                .assertValueCount(1)
                .assertValue(Batch.of("resource1", updatesList))
                .assertCompleted();
    }

    @Test
    public void keepUpdatesWithHighestPriorityInEachBatch() {
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final Instant moreRecent = now.plus(ofMillis(1));
        final BatchableOperationMock lowPriority = new BatchableOperationMock(Low, moreRecent, "resource3", "sub2", "create");
        final BatchableOperationMock highPriority = new BatchableOperationMock(High, now, "resource3", "sub2", "remove");

        assertEmitSingleAfterReceiving(highPriority, lowPriority, highPriority);
    }

    @Test
    public void keepMostRecentUpdatesInEachBatch() {
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final Instant moreRecent = now.plus(ofMillis(1));
        final BatchableOperationMock old = new BatchableOperationMock(Low, now, "resource2", "sub2", "create");
        final BatchableOperationMock recent = new BatchableOperationMock(Low, moreRecent, "resource2", "sub2", "remove");

        assertEmitSingleAfterReceiving(recent, old, recent);
    }

    @Test
    public void doNotReplaceOlderUpdatesDoingTheSame() {
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final Instant moreRecent = now.plus(ofSeconds(10));
        final BatchableOperationMock old = new BatchableOperationMock(Low, now, "resource1", "sub2", "create");
        final BatchableOperationMock recentDoingTheSame = new BatchableOperationMock(Low, moreRecent, "resource1", "sub2", "create");

        assertEmitSingleAfterReceiving(old, old, recentDoingTheSame);
    }

    private void assertEmitSingleAfterReceiving(BatchableOperationMock expected, BatchableOperationMock... receiving) {
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(minimumTimeInQueueMs);
        final Subject<BatchableOperationMock, BatchableOperationMock> updates = PublishSubject.<BatchableOperationMock>create().toSerialized();

        final AssertableSubscriber<Batch<BatchableOperationMock, String>> subscriber = updates.lift(batcher).test();
        testScheduler.triggerActions();
        subscriber.assertNoTerminalEvent().assertNoValues();

        for (BatchableOperationMock received : receiving) {
            updates.onNext(received);
        }

        testScheduler.advanceTimeBy(2 * minimumTimeInQueueMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoErrors()
                .assertValueCount(1)
                .assertValue(Batch.of(expected.getResourceId(), Collections.singletonList(expected)));
    }

    @Test
    public void onCompletedIsNotSentAfterOnError() {
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(minimumTimeInQueueMs);
        final Subject<BatchableOperationMock, BatchableOperationMock> updates = PublishSubject.<BatchableOperationMock>create().toSerialized();

        final AssertableSubscriber<Batch<BatchableOperationMock, String>> subscriber = updates.lift(batcher).test();
        testScheduler.triggerActions();
        subscriber.assertNoTerminalEvent().assertNoValues();

        updates.onError(new RuntimeException("some problem"));
        testScheduler.triggerActions(); // onError is forwarded right away (i.e.: don't wait for the next flush event)
        subscriber.assertNotCompleted().assertError(RuntimeException.class);

        updates.onCompleted();
        subscriber.assertNotCompleted();
    }

    @Test
    public void onErrorIsNotSentAfterOnCompleted() {
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(minimumTimeInQueueMs);
        final Subject<BatchableOperationMock, BatchableOperationMock> updates = PublishSubject.<BatchableOperationMock>create().toSerialized();

        final AssertableSubscriber<Batch<BatchableOperationMock, String>> subscriber = updates.lift(batcher).test();
        testScheduler.triggerActions();
        subscriber.assertNoTerminalEvent().assertNoValues();

        updates.onCompleted();
        // onCompleted is sent after pending items are drained
        testScheduler.advanceTimeBy(minimumTimeInQueueMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoErrors().assertCompleted();

        updates.onError(new RuntimeException("some problem"));
        subscriber.assertNoErrors();
    }

    @Test
    public void ignoreErrorsFromDownstream() {
        final Instant now = Instant.ofEpochMilli(testScheduler.now());
        final RateLimitedBatcher<BatchableOperationMock, String> batcher = buildBatcher(minimumTimeInQueueMs);
        final Subject<BatchableOperationMock, BatchableOperationMock> updates = PublishSubject.<BatchableOperationMock>create().toSerialized();

        final AssertableSubscriber<?> subscriber = updates.lift(batcher)
                .lift(new ExceptionThrowingOperator("some error happened"))
                .test();
        testScheduler.triggerActions();
        subscriber.assertNoTerminalEvent().assertNoValues();

        for (int i = 0; i < 10; i++) {
            updates.onNext(new BatchableOperationMock(Low, now, "resource2", "sub2", "create"));
            testScheduler.advanceTimeBy(minimumTimeInQueueMs, TimeUnit.MILLISECONDS);
            subscriber.assertNoTerminalEvent().assertNoValues();
        }

        updates.onCompleted();
        testScheduler.advanceTimeBy(minimumTimeInQueueMs, TimeUnit.MILLISECONDS);
        subscriber.assertNoValues().assertCompleted();
    }

    private <T extends Batchable<I>, I> List<T> toUpdateList(List<Batch<T, I>> expected) {
        List<T> updates = expected.stream()
                .flatMap(batch -> batch.getItems().stream())
                .collect(Collectors.toList());
        Collections.shuffle(updates); // ensure we don't rely on the ordering of updates
        return updates;
    }

    private Instant randomWithinBucket(Instant when) {
        return when.plus(ofMillis(random.nextInt(timeWindowBucketSizeMs)));

    }

    private RateLimitedBatcher<BatchableOperationMock, String> buildBatcher(long initialDelayMs) {
        return buildBatcher(initialDelayMs, Long.MAX_VALUE);
    }

    private RateLimitedBatcher<BatchableOperationMock, String> buildBatcher(long initialDelayMs, long maxDelayMs) {
        return RateLimitedBatcher.create(tokenBucket, initialDelayMs, maxDelayMs,
                BatchableOperationMock::getResourceId, strategy, "testBatcher", new NoopRegistry(), testScheduler);
    }

    private static class ExceptionThrowingOperator implements Observable.Operator<Object, Batch<?, ?>> {
        private final String errorMessage;

        private ExceptionThrowingOperator(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        @Override
        public Subscriber<? super Batch<?, ?>> call(Subscriber<? super Object> child) {
            return new Subscriber<Batch<?, ?>>() {
                @Override
                public void onCompleted() {
                    child.onCompleted();
                }

                @Override
                public void onError(Throwable e) {
                    child.onError(e);
                }

                @Override
                public void onNext(Batch<?, ?> batch) {
                    throw new RuntimeException(errorMessage);
                }
            };
        }
    }
}