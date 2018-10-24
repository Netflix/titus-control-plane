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

package com.netflix.titus.supplementary.relocation.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.supplementary.relocation.model.DeschedulingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.supplementary.relocation.util.RelocationUtil.doFormat;

class DeschedulingResultLogger {

    private static final Logger logger = LoggerFactory.getLogger("DeschedulerLogger");

    private static final long LOGGING_INTERVAL_MS = 100;

    private final TokenBucket loggingTokenBucket = Limiters.createFixedIntervalTokenBucket(
            DeschedulingResultLogger.class.getSimpleName(),
            1,
            1,
            1,
            LOGGING_INTERVAL_MS,
            TimeUnit.MILLISECONDS
    );

    void doLog(Map<String, DeschedulingResult> deschedulingResult) {
        if (!loggingTokenBucket.tryTake()) {
            return;
        }

        if (deschedulingResult.isEmpty()) {
            logger.info("Descheduler result: empty");
            return;
        }

        Map<String, List<DeschedulingResult>> byAgentEvictable = new HashMap<>();
        Map<String, List<DeschedulingResult>> byAgentNotEvictable = new HashMap<>();

        deschedulingResult.values().forEach(d -> {
            if (d.canEvict()) {
                byAgentEvictable.computeIfAbsent(d.getAgentInstance().getId(), i -> new ArrayList<>()).add(d);
            } else {
                byAgentNotEvictable.computeIfAbsent(d.getAgentInstance().getId(), i -> new ArrayList<>()).add(d);
            }
        });

        long toEvictCount = deschedulingResult.values().stream().filter(DeschedulingResult::canEvict).count();
        long failureCount = deschedulingResult.size() - toEvictCount;

        logger.info("Descheduler result: evictable={}, failures={}", toEvictCount, failureCount);
        if (toEvictCount > 0) {
            logger.info("    Evictable tasks:");
            byAgentEvictable.forEach((agentId, results) -> {
                logger.info("        Agent({}):", agentId);
                results.forEach(result ->
                        logger.info("           task({}): {}", result.getTask().getId(), doFormat(result.getTaskRelocationPlan()))
                );
            });
        }
        if (failureCount > 0) {
            logger.info("    Not evictable tasks (failures):");
            byAgentNotEvictable.forEach((agentId, results) -> {
                logger.info("        Agent(%s):", agentId);
                results.forEach(result ->
                        logger.info("           task({}): failure={}, plan={}",
                                result.getTask().getId(), result.getFailure().get().getReasonMessage(),
                                doFormat(result.getTaskRelocationPlan())
                        )
                );
            });
        }
    }
}
