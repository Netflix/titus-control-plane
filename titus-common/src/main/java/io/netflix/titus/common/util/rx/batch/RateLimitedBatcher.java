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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.collections.ConcurrentHashMultimap;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.plugins.RxJavaHooks;


/**
 * rxJava operator that buffers items from an upstream Observable stream, and emits batches indexed by pluggable logic
 * to downstream subscribers, in a rate provided by a {@link TokenBucket}.
 * <p>
 * Items for a particular index are batched for a minimum period (<tt>initialDelay</tt>). When the {@link TokenBucket}
 * gets exhausted, emission of batches to downstream subscribers is paused with an exponential backoff strategy up to
 * <tt>maxDelay</tt>.
 * <p>
 * Items are removed from the (in-memory) pending buffer right after they are emitted, but concurrent operations are
 * allowed while batches are being emitted (flushed). It is possible that a particular item is replaced in the in-memory
 * buffer while it is being emitted. To avoid losing the most recent item, they are compared with
 * {@link Object#equals(Object)} and removed atomically from the buffer only when considered equal
 * (with <tt>compareAndRemove</tt> semantics). Implementations of {@link Batchable} are encouraged to implement
 * <tt>equals</tt> in a way that two {@link Batchable} causing the system to be in the same final state are considered
 * equal.
 * <p>
 * A single instance of this operator can be used in multiple different rxJava streams, in which case the same
 * {@link TokenBucket} will be shared across all of them, and all work will be scheduled on the same {@link Scheduler.Worker}.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 * RateLimitedBatcher<SomeOperation, IndexType> batcher = RateLimitedBatcher.create(...);
 * Observable<SomeOperation> source = ...;
 * source.lift(RateLimitedBatcher.create(...))
 *     .subscribe(Batch<SomeOperation, IndexType> batch -> {
 *         // batch.getIndex() is of type IndexType, and will be provided by an IndexExtractor
 *         processBatch(batch.getItems());
 *     });
 * }
 * </pre>
 *
 * @param <T> type of items to be queued and batched
 * @param <I> type of the value (an index, usually a field of T, provided by <tt>IndexExtractor</tt>) to batch by
 */
public class RateLimitedBatcher<T extends Batchable<?>, I> implements Observable.Operator<Batch<T, I>, T> {
    private final Logger logger = LoggerFactory.getLogger(RateLimitedBatcher.class);

    private final Scheduler.Worker worker;
    private final TokenBucket tokenBucket;
    private final IndexExtractor<T, I> indexExtractor;
    private final EmissionStrategy emissionStrategy;

    private final long initialDelayMs;
    private final long maxDelayMs;

    public static <T extends Batchable<?>, I>
    RateLimitedBatcher<T, I> create(Scheduler scheduler, TokenBucket tokenBucket, long initialDelay, long maxDelay,
                                    IndexExtractor<T, I> indexExtractor, EmissionStrategy emissionStrategy) {
        return new RateLimitedBatcher<T, I>(scheduler, tokenBucket, initialDelay, maxDelay, indexExtractor, emissionStrategy);
    }

    private RateLimitedBatcher(Scheduler scheduler, TokenBucket tokenBucket, long initialDelay, long maxDelay,
                               IndexExtractor<T, I> indexExtractor, EmissionStrategy emissionStrategy) {
        Preconditions.checkArgument(initialDelay > 0, "initialDelayMs must be > 0");
        Preconditions.checkArgument(maxDelay >= initialDelay, "maxDelayMs must be >= initialDelayMs");
        this.tokenBucket = tokenBucket;
        this.initialDelayMs = initialDelay;
        this.maxDelayMs = maxDelay;
        this.indexExtractor = indexExtractor;
        this.emissionStrategy = emissionStrategy;
        // TODO(fabio): change this to InstrumentedEventLoop when it gets merged
        this.worker = scheduler.createWorker();
    }

    @Override
    public Subscriber<? super T> call(final Subscriber<? super Batch<T, I>> downstream) {
        Flusher flusher = new Flusher(downstream);
        flusher.run();
        return new BatchUpstreamSubscriber(flusher);
    }

    private final class BatchUpstreamSubscriber extends Subscriber<T> {
        private final Flusher flusher;

        private BatchUpstreamSubscriber(Flusher flusher) {
            this.flusher = flusher;
        }

        @Override
        public void onCompleted() {
            flusher.stopWhenEmpty();
        }

        @Override
        public void onError(Throwable e) {
            flusher.sendError(e);
        }

        @Override
        public void onNext(T item) {
            Preconditions.checkNotNull(item);
            flusher.offer(item);
        }
    }

    /**
     * Continually flushes pending batches to downstream subscribers
     */
    // FIXME: replace synchronized loops with an event loop (action queue) to respect the Observable contract
    private final class Flusher {
        /**
         * each batch is an immutable Map (indexed by <tt>Batchable#getIdentifier()</tt>), and modifications are applied
         * with copy-on-write
         */
        private final ConcurrentHashMultimap<I, T> pending =
                new ConcurrentHashMultimap<>(Batchable::getIdentifier, this::isHigherPriorityOrNewer);
        /**
         * all calls to on{Next,Error,Completed} need to be serialized in a synchronized(this) block so we respect the
         * <tt>Observable</tt> protocol (onError or onCompleted are called only once, and no items are delivered with
         * onNext after they are called).
         */
        private final Subscriber<? super Batch<T, I>> downstream;

        /**
         * current delay between scans for pending items to be flushed. It is exponentially incremented on errors, or
         * when we get rate limited by the <tt>TokenBucket</tt>
         */
        private final AtomicLong currentDelayMs = new AtomicLong(initialDelayMs);
        /**
         * tracks when upstream has completed emitting, so we can terminate after flushing what is currently pending
         */
        private volatile boolean done = false;
        /**
         * tracks when an error event (onError) has been sent to downstream subscribers, so we don't try and send more
         * items. Access to it needs to be synchronized on <tt>this</tt>
         */
        private volatile Throwable sentError;

        private Flusher(Subscriber<? super Batch<T, I>> downstream) {
            this.downstream = downstream;
        }

        /**
         * On a conflict, replace the existing value if the new one has higher priority, or the same priority but is
         * different (as per {@link Batchable#isEquivalent(Batchable)}) and a more recent timestamp. That way, older
         * items are only replaced by newer items doing something different.
         * <p>
         * Same priority and same timestamp is also replaced (last with the same timestamp wins) as long as they are
         * different (by {@link Batchable#isEquivalent(Batchable)}).
         *
         * @param existing    old value
         * @param replacement new value
         * @return true when the existing value should be replaced by the new value
         */
        private boolean isHigherPriorityOrNewer(T existing, T replacement) {
            final int priorityComparison = Batchable.byPriority().compare(replacement, existing);
            if (priorityComparison != 0) {
                return priorityComparison > 0; // always keep the one with higher priority
            }
            // same priority, check if newer
            boolean isMoreRecent = !replacement.getTimestamp().isBefore(existing.getTimestamp());
            return isMoreRecent && !replacement.isEquivalent(existing);
        }

        /**
         * start the continuous loop
         */
        public void run() {
            worker.schedule(this::flushPending, currentDelayMs.get(), TimeUnit.MILLISECONDS);
        }

        /**
         * continually schedules itself to keep flushing items being accumulated
         */
        private void flushPending() {
            synchronized (this) {
                if (whenErrorSentTerminate()) {
                    logger.info("Ending the flush loop, onError was called and onNext can not be called anymore");
                    return;
                }
            }

            Queue<Batch<T, I>> ordered = emissionStrategy.compute(readyBatchesStream());
            if (ordered.isEmpty()) {
                scheduleNextIfNotDone();
                return;
            }

            for (; ; ) {
                final Batch<T, I> next = ordered.poll();
                if (next == null) {
                    break;
                }

                if (!tokenBucket.tryTake()) {
                    scheduleNextWhenRateLimited();
                    return;
                }
                resetCurrentDelay();

                synchronized (this) {
                    if (whenErrorSentTerminate()) {
                        logger.info("Ending the flush loop, onError was called and onNext can not be called anymore");
                        return;
                    }
                    onNextSafe(next);
                }
                /*
                 * Only remove sent items if they have not been modified in the pending data structure to avoid losing
                 * items that were replaced while being emitted.
                 *
                 * Partial failures in the batch consider the whole batch as done.
                 */
                next.getItems().forEach(item -> pending.removeIf(next.getIndex(), item,
                        current -> current.isEquivalent(item)
                ));
            }

            logger.debug("All pending items flushed, scheduling another round");
            worker.schedule(this::flushPending);
        }

        /**
         * swallow downstream exceptions and keep the event loop running
         */
        private void onNextSafe(Batch<T, I> next) {
            // TODO: consider terminating the event loop, unsubscribing from upstream and sending an onError downstream
            // TODO: capture rate limit exceptions from downstream and apply exponential backoff here too
            try {
                downstream.onNext(next);
            } catch (Throwable ex) {
                Exceptions.throwIfFatal(ex);
                logger.error("onNext failed, ignoring batch " + next.getIndex().toString(), ex);
            }
        }

        private void scheduleNextWhenRateLimited() {
            final long nextRefill = tokenBucket.getRefillStrategy().getTimeUntilNextRefill(TimeUnit.MILLISECONDS);
            final long delayForNext = Math.max(increaseAndGetCurrentDelayMs(), nextRefill);
            logger.warn("Rate limit applied, retry in {} ms", delayForNext);
            worker.schedule(this::flushPending, delayForNext, TimeUnit.MILLISECONDS);
        }

        private void scheduleNextIfNotDone() {
            if (done && pending.isEmpty()) {
                logger.info("Ending the flush loop, all pending items were flushed after onComplete from upstream");
                sendCompleted();
                return;
            }
            logger.debug("No batches are ready yet. Next iteration in {} ms", currentDelayMs);
            worker.schedule(this::flushPending, currentDelayMs.get(), TimeUnit.MILLISECONDS);
        }

        private long increaseAndGetCurrentDelayMs() {
            return currentDelayMs.updateAndGet(current -> Math.min(maxDelayMs, current << 1));
        }

        /**
         * reset backoff when we have tokens again
         */
        private void resetCurrentDelay() {
            currentDelayMs.set(initialDelayMs);
        }

        /**
         * Let batches accumulate in pending for at least initialDelayMs
         */
        private Stream<Batch<T, I>> readyBatchesStream() {
            return pending.asMap().entrySet().stream()
                    // TODO: Batch.of() iterates on all values to find oldestTimestamp. Consider precomputing as they are added
                    .map(entry -> Batch.of(entry.getKey(), new ArrayList<>(entry.getValue())))
                    .filter(batch -> isWaitingForAtLeast(batch, initialDelayMs));
        }

        private boolean isWaitingForAtLeast(Batch<T, I> batch, long ms) {
            Instant now = Instant.ofEpochMilli(worker.now());
            final Instant cutLine = now.minus(ms, ChronoUnit.MILLIS);
            return !batch.getOldestItemTimestamp().isAfter(cutLine);
        }

        /**
         * calls to this must be inside a synchronized(this) block
         */
        private boolean whenErrorSentTerminate() {
            if (sentError != null) {
                worker.unsubscribe();
                return true;
            }
            return false;
        }

        private synchronized void sendError(Throwable e) {
            // TODO: check if onCompleted was called
            if (sentError != null) {
                logger.error("Another error was already sent, emitting as undeliverable", e);
                RxJavaHooks.onError(e);
                return;
            }

            sentError = e;
            /*
             * setting done is not strictly necessary since the error tracking above should be enough to stop periodic
             * scheduling, but it is here for completeness
             */
            done = true;
            downstream.onError(e);
        }

        /**
         * ensure onCompleted is only called if onError was not (the {@link Observable} contract)
         */
        private synchronized void sendCompleted() {
            if (sentError == null) {
                downstream.onCompleted();
            }
            worker.unsubscribe();
        }

        /**
         * causes current pending items to be drained to downstream subscribers before stopping completely
         */
        private void stopWhenEmpty() {
            done = true;
        }

        /**
         * enqueue an item from upstream
         */
        private void offer(T item) {
            if (done) {
                return; // don't accumulate more after being told to stop
            }
            synchronized (this) {
                if (sentError != null) {
                    return; // no point in accumulating more
                }
            }
            pending.put(indexExtractor.apply(item), item);
        }
    }
}
