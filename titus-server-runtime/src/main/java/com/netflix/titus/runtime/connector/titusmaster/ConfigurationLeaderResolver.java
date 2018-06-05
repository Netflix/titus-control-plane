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

package com.netflix.titus.runtime.connector.titusmaster;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.PublishSubject;

@Singleton
public class ConfigurationLeaderResolver implements LeaderResolver {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLeaderResolver.class);
    private static final int LEADER_REFRESH_INTERVAL_MS = 5_000;

    private final ScheduledExecutorService executorService;

    private final PublishSubject<Optional<Address>> leaderPublishSubject;
    private volatile Address leaderAddress;

    @Inject
    public ConfigurationLeaderResolver(TitusMasterClientConfiguration configuration) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("configuration-leader-resolver-%d").build();
        this.executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
        leaderPublishSubject = PublishSubject.create();

        executorService.scheduleAtFixedRate(() -> {
            try {
                logger.debug("Current leaderAddress: {}", leaderAddress);
                Address newLeaderAddress = new Address(configuration.getMasterScheme(), configuration.getMasterIp(), configuration.getMasterHttpPort());
                if (!Objects.equals(leaderAddress, newLeaderAddress)) {
                    logger.info("Updating leaderAddress from: {} to: {}", leaderAddress, newLeaderAddress);
                    leaderAddress = newLeaderAddress;
                }
                leaderPublishSubject.onNext(Optional.of(newLeaderAddress));
            } catch (Exception e) {
                logger.error("Unable to resolve current titus master with error: ", e);
            }
        }, 0, LEADER_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdownNow();
    }

    public Optional<Address> resolve() {
        return Optional.of(leaderAddress);
    }

    @Override
    public Observable<Optional<Address>> observeLeader() {
        return leaderPublishSubject;
    }
}
