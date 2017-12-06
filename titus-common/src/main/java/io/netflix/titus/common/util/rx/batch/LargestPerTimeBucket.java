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
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Stream;

import rx.Scheduler;

/**
 * Emit batches with older items first. Bigger batches with "similar" timestamps are allowed to be emitted first, by
 * grouping all batches into time buckets, based on a configurable time window.
 * <p>
 * This allows items sitting on the queue for longer to be processed first, while optimistically grouping some newer
 * items that can be batched together with it. Configurable time window buckets allow for tunable favoring of bigger
 * batches, for cases where it is better to process bigger batches in front of smaller but slightly older batches.
 * <p>
 * This {@link EmissionStrategy} also allows for batches that have not been waiting in the queue for a minimum period
 * to be filtered out and not emitted, allowing batches to accumulate items for a minimum configurable period of time.
 */
public class LargestPerTimeBucket implements EmissionStrategy {
    private final long minimumTimeInQueueMs;
    private final long timeWindowBucketSizeMs;
    private final Scheduler scheduler;

    public LargestPerTimeBucket(long minimumTimeInQueueMs, long timeWindowBucketSizeMs, Scheduler scheduler) {
        this.minimumTimeInQueueMs = minimumTimeInQueueMs;
        this.timeWindowBucketSizeMs = timeWindowBucketSizeMs;
        this.scheduler = scheduler;
    }

    /**
     * Compute the order in which batches should be emitted.
     *
     * @param batches all batches to be emitted
     * @param <T>     type of updates in the batch
     * @param <I>     type of the index of each batch
     * @return a <tt>Queue</tt> representing the order in which batches should be emitted
     */
    @Override
    public <T extends Update<?>, I> Queue<Batch<T, I>> compute(Stream<Batch<T, I>> batches) {
        // TODO: break too large batches by a configurable maxBatchSize

        final Instant now = Instant.ofEpochMilli(scheduler.now());
        final Instant cutLine = now.minus(minimumTimeInQueueMs, ChronoUnit.MILLIS);
        final Stream<Batch<T, I>> inQueueMinimumRequired = batches.filter(
                batch -> !batch.getOldestItemTimestamp().isAfter(cutLine)
        );

        PriorityQueue<Batch<T, I>> queue = new PriorityQueue<>(this::compare);
        inQueueMinimumRequired.forEach(queue::offer);
        return queue;
    }

    private <T extends Update<?>, I> int compare(Batch<T, I> one, Batch<T, I> other) {
        final long oneTimeBucket = bucketFor(one.getOldestItemTimestamp());
        final long otherTimeBucket = bucketFor(other.getOldestItemTimestamp());

        // older first
        final int timeBucketComparison = Long.compare(oneTimeBucket, otherTimeBucket);
        if (timeBucketComparison == 0) {
            // in the same bucket, bigger batches first
            return Batch.bySize().reversed().compare(one, other);
        }
        return timeBucketComparison;
    }

    private long bucketFor(Instant timestamp) {
        return timestamp.toEpochMilli() / timeWindowBucketSizeMs;
    }
}
