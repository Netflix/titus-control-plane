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
     * Compute the order in which candidates should be emitted.
     *
     * @param candidates all candidates to be emitted
     * @param <T>        type of items in the batch
     * @param <I>        type of the index of each batch
     * @return a <tt>Queue</tt> representing the order in which candidates should be emitted
     */
    @Override
    public <T extends Batchable<?>, I> Queue<Batch<T, I>> compute(Stream<Batch<T, I>> candidates) {
        // TODO: break too large candidates by a configurable maxBatchSize

        final Instant now = Instant.ofEpochMilli(scheduler.now());
        final Instant cutLine = now.minus(minimumTimeInQueueMs, ChronoUnit.MILLIS);
        final Stream<Batch<T, I>> inQueueMinimumRequired = candidates.filter(
                batch -> !batch.getOldestItemTimestamp().isAfter(cutLine)
        );

        PriorityQueue<Batch<T, I>> queue = new PriorityQueue<>(this::compare);
        inQueueMinimumRequired.forEach(queue::offer);
        return queue;
    }

    private <T extends Batchable<?>, I> int compare(Batch<T, I> one, Batch<T, I> other) {
        final Instant oneTimestamp = one.getOldestItemTimestamp();
        final Instant otherTimestamp = other.getOldestItemTimestamp();
        final long oneTimeBucket = bucketFor(oneTimestamp);
        final long otherTimeBucket = bucketFor(otherTimestamp);

        // older first
        final int timeBucketComparison = Long.compare(oneTimeBucket, otherTimeBucket);
        if (timeBucketComparison != 0) {
            return timeBucketComparison;
        }

        // in the same bucket, bigger batches first
        final int sizeComparison = Batch.bySize().reversed().compare(one, other);
        if (sizeComparison != 0) {
            return sizeComparison;
        }
        // unless they have the same size in the same bucket, in which case order by timestamp
        return oneTimestamp.compareTo(otherTimestamp);
    }

    private long bucketFor(Instant timestamp) {
        return timestamp.toEpochMilli() / timeWindowBucketSizeMs;
    }
}
