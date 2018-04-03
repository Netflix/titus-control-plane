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

package com.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Scheduler;
import rx.Subscription;

public class ContainerPlayersManager {

    private static final Logger logger = LoggerFactory.getLogger(ContainerPlayersManager.class);

    private static final long CLEANUP_INTERVAL_MS = 10_000;

    private final ConcurrentMap<String, JobPlayer> jobPlayers = new ConcurrentHashMap<>();
    private final Scheduler scheduler;
    private final Subscription cleanupSubscription;

    public ContainerPlayersManager(Registry registry, Scheduler scheduler) {
        this.scheduler = scheduler;
        this.cleanupSubscription = ObservableExt.schedule("cloudSimulator.containerPlayersManager",
                registry,
                "cleanup",
                Completable.fromAction(this::removeStaleJobPlayers),
                CLEANUP_INTERVAL_MS,
                CLEANUP_INTERVAL_MS,
                TimeUnit.MILLISECONDS,
                scheduler
        ).subscribe(
                next -> next.ifPresent(e -> logger.warn("Cleanup iteration failure", e)),
                e -> logger.warn("Unexpected error", e),
                () -> logger.info("ContainerPlayersManager completed")
        );
    }

    public void shutdown() {
        cleanupSubscription.unsubscribe();
        jobPlayers.values().forEach(JobPlayer::shutdown);
    }

    public boolean play(TaskExecutorHolder taskHolder) {
        JobPlayer jobPlayer = jobPlayers.get(taskHolder.getJobId());
        if (jobPlayer == null) {
            List<Pair<ContainerSelector, ContainerRules>> parseResult = PlayerParser.parse(taskHolder.getEnv());
            if (parseResult.isEmpty()) {
                return false;
            }
            jobPlayer = new JobPlayer(parseResult, scheduler);
            jobPlayers.put(taskHolder.getJobId(), jobPlayer);
        }
        jobPlayer.play(taskHolder);
        return true;
    }

    private void removeStaleJobPlayers() {
        jobPlayers.values().removeIf(JobPlayer::isStale);
    }
}
