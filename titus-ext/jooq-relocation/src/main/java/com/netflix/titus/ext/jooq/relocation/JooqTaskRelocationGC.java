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

package com.netflix.titus.ext.jooq.relocation;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.titus.api.common.LeaderActivationListener;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task relocation garbage collector. Removes entries from the relocation status table older than a configurable
 * amount of time.
 */
@Singleton
public class JooqTaskRelocationGC implements LeaderActivationListener {

    public static String METRIC_ROOT = "titus.relocation.gc.";

    private static final Logger logger = LoggerFactory.getLogger(JooqTaskRelocationGC.class);

    private final JooqRelocationConfiguration configuration;
    private final DSLContext dslContext;
    private final JooqTaskRelocationResultStore relocationResultStore;
    private final TitusRuntime titusRuntime;

    private ScheduleReference scheduleRef;

    private final Gauge allRowsGauge;
    private final Gauge expiredRowsGauge;
    private final Counter gcCounter;

    @Inject
    public JooqTaskRelocationGC(JooqRelocationConfiguration configuration,
                                DSLContext dslContext,
                                TaskRelocationResultStore relocationResultStore,
                                TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.dslContext = dslContext;
        this.relocationResultStore = (JooqTaskRelocationResultStore) relocationResultStore;
        this.titusRuntime = titusRuntime;
        this.allRowsGauge = titusRuntime.getRegistry().gauge(METRIC_ROOT + "allRows", "table", "relocation_status");
        this.expiredRowsGauge = titusRuntime.getRegistry().gauge(METRIC_ROOT + "expiredRows", "table", "relocation_status");
        this.gcCounter = titusRuntime.getRegistry().counter(METRIC_ROOT + "removedCount", "table", "relocation_status");
    }

    @Override
    public void activate() {
        this.scheduleRef = titusRuntime.getLocalScheduler().schedule(ScheduleDescriptor.newBuilder()
                        .withName(JooqTaskRelocationGC.class.getSimpleName())
                        .withDescription("Titus relocation data GC")
                        .withInitialDelay(Duration.ZERO)
                        .withInterval(configuration.getGcInterval())
                        .withTimeout(Duration.ofMinutes(30))
                        .build(),
                context -> doGC(),
                true
        );
    }

    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(scheduleRef, ScheduleReference::cancel);
    }

    private void doGC() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        logger.info("Starting new relocation status table GC cycle...");
        removeExpiredData(titusRuntime.getClock().wallTime() - configuration.getRetentionTime().toMillis());
        logger.info("The relocation status table GC cycle finished: elapsedMs={}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * Removes all entries older than the given time threshold.
     */
    @VisibleForTesting
    int removeExpiredData(long timeThreshold) {
        // Count all items
        int allCount = dslContext.fetchCount(Relocation.RELOCATION.RELOCATION_STATUS);
        logger.info("All rows in 'relocation_status' table: {}", allCount);
        allRowsGauge.set(allCount);

        int expiredCount = dslContext.fetchCount(
                Relocation.RELOCATION.RELOCATION_STATUS,
                Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME.lt(Timestamp.from(Instant.ofEpochMilli(timeThreshold)))
        );
        logger.info("Expired rows in 'relocation_status' table: {}", expiredCount);
        expiredRowsGauge.set(expiredCount);

        if (expiredCount <= 0) {
            return 0;
        }

        // Locate timestamp from which to remove.
        Result<Record2<String, Timestamp>> timestampRow = dslContext.select(Relocation.RELOCATION.RELOCATION_STATUS.TASK_ID, Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME)
                .from(Relocation.RELOCATION.RELOCATION_STATUS)
                .where(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME.lt(Timestamp.from(Instant.ofEpochMilli(timeThreshold))))
                .orderBy(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME.asc())
                .limit(configuration.getGcRowLimit())
                .fetch();

        if (timestampRow.isEmpty()) {
            logger.info("No expired data found");
            return 0;
        }
        Timestamp lastToRemove = timestampRow.get(timestampRow.size() - 1).getValue(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME);

        // Load all data up to the given timestamp. This could be more data than above (and more than the GC limit when
        // there are records with the lastToRemove timestamp, which were not returned due to the limit constraint.
        // This is fine, as we do not expect that there are too many like this.
        Result<Record2<String, Timestamp>> toRemoveRows = dslContext.select(Relocation.RELOCATION.RELOCATION_STATUS.TASK_ID, Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME)
                .from(Relocation.RELOCATION.RELOCATION_STATUS)
                .where(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME.le(lastToRemove))
                .orderBy(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME.asc())
                .fetch();

        List<Pair<String, Long>> toRemoveSet = toRemoveRows.stream()
                .map(r -> Pair.of(r.get(Relocation.RELOCATION.RELOCATION_STATUS.TASK_ID), r.get(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME).getTime()))
                .collect(Collectors.toList());
        logger.info("Records to remove: {}", toRemoveSet);

        int removedFromDb = dslContext.delete(Relocation.RELOCATION.RELOCATION_STATUS).where(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME.le(lastToRemove)).execute();

        logger.info("Removed expired rows from 'relocation_status' table: {}", removedFromDb);
        gcCounter.increment(removedFromDb);

        relocationResultStore.removeFromCache(toRemoveSet);

        return removedFromDb;
    }
}
