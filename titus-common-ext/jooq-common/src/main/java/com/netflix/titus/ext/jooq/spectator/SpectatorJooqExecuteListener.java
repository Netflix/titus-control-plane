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

package com.netflix.titus.ext.jooq.spectator;

import com.netflix.spectator.api.Id;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.spectator.MetricSelector;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.spectator.ValueRangeCounter;
import com.netflix.titus.common.util.time.Clock;
import org.jooq.ExecuteContext;
import org.jooq.ExecuteListener;

public class SpectatorJooqExecuteListener implements ExecuteListener {

    private final MetricSelector<ValueRangeCounter> latencies;
    private final Clock clock;

    private final ThreadLocal<Long> startTimestamp = new ThreadLocal<>();
    private final ThreadLocal<Long> executeTimestamp = new ThreadLocal<>();

    public SpectatorJooqExecuteListener(Id root, TitusRuntime titusRuntime) {
        this.clock = titusRuntime.getClock();
        this.latencies = SpectatorExt.newValueRangeCounter(
                titusRuntime.getRegistry().createId(root.name() + "latencies", root.tags()),
                new String[]{"point"},
                SpectatorUtils.LEVELS,
                titusRuntime.getRegistry()
        );
    }

    /**
     * Called at the very beginning in Jooq AbstractQuery. Captures the total time of query execution within Jooq
     * including the time it gets to get a free connection from the pool.
     */
    @Override
    public void start(ExecuteContext ctx) {
        startTimestamp.set(clock.wallTime());
    }

    @Override
    public void renderStart(ExecuteContext ctx) {

    }

    @Override
    public void renderEnd(ExecuteContext ctx) {

    }

    @Override
    public void prepareStart(ExecuteContext ctx) {

    }

    @Override
    public void prepareEnd(ExecuteContext ctx) {

    }

    @Override
    public void bindStart(ExecuteContext ctx) {

    }

    @Override
    public void bindEnd(ExecuteContext ctx) {

    }

    /**
     * Called just before executing a statement, after connection was acquired and all parameters were bound.
     */
    @Override
    public void executeStart(ExecuteContext ctx) {
        executeTimestamp.set(clock.wallTime());
    }

    /**
     * See {@link #executeStart(ExecuteContext)}
     */
    @Override
    public void executeEnd(ExecuteContext ctx) {
        recordLatency(executeTimestamp, "execute");
    }

    @Override
    public void outStart(ExecuteContext ctx) {

    }

    @Override
    public void outEnd(ExecuteContext ctx) {

    }

    @Override
    public void fetchStart(ExecuteContext ctx) {

    }

    @Override
    public void resultStart(ExecuteContext ctx) {

    }

    @Override
    public void recordStart(ExecuteContext ctx) {

    }

    @Override
    public void recordEnd(ExecuteContext ctx) {

    }

    @Override
    public void resultEnd(ExecuteContext ctx) {

    }

    @Override
    public void fetchEnd(ExecuteContext ctx) {

    }

    /**
     * See {@link #start(ExecuteContext)}
     */
    @Override
    public void end(ExecuteContext ctx) {
        recordLatency(startTimestamp, "start");
    }

    private void recordLatency(ThreadLocal<Long> timestamp, String point) {
        Long startTime = timestamp.get();
        if (startTime != null) {
            latencies.withTags(point).ifPresent(m -> m.recordLevel(clock.wallTime() - startTime));
        }
    }

    @Override
    public void exception(ExecuteContext ctx) {

    }

    @Override
    public void warning(ExecuteContext ctx) {

    }
}
