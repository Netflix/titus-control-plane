/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.clustermembership.service;

import java.time.Duration;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import com.netflix.titus.common.util.ExceptionExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class ClusterMembershipTransactionLogger {

    private static final Logger logger = LoggerFactory.getLogger("ClusterMembershipTransactionLogger");

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private static final int BUFFER_SIZE = 5000;

    static Disposable logEvents(Flux<ClusterMembershipEvent> events) {
        Scheduler scheduler = Schedulers.newSingle("ClusterMembershipTransactionLogger");
        return events
                .onBackpressureBuffer(BUFFER_SIZE, BufferOverflowStrategy.ERROR)
                .publishOn(scheduler)
                .onErrorResume(e -> {
                    logger.warn("Transactions may be missing in the log. The event stream has terminated with an error and must be re-subscribed: {}", ExceptionExt.toMessage(e));
                    return Flux.interval(RETRY_INTERVAL).take(1).flatMap(tick -> events);
                })
                .subscribe(
                        event -> logger.info(doFormat(event)),
                        e -> logger.error("Event stream terminated with an error", e),
                        () -> logger.info("Event stream completed")
                );
    }

    @VisibleForTesting
    static String doFormat(ClusterMembershipEvent event) {
        if (event instanceof ClusterMembershipChangeEvent) {
            return doFormat((ClusterMembershipChangeEvent) event);
        }
        if (event instanceof LeaderElectionChangeEvent) {
            return doFormat((LeaderElectionChangeEvent) event);
        }
        return null;
    }

    private static String doFormat(ClusterMembershipChangeEvent event) {
        ClusterMember member = event.getRevision().getCurrent();
        return doFormat(
                "membership",
                member.getMemberId(),
                member.isActive() + "",
                member.isRegistered() + "",
                member.isEnabled() + "",
                event.getRevision().getRevision() + "",
                "n/a",
                "n/a"
        );
    }


    private static String doFormat(LeaderElectionChangeEvent event) {
        ClusterMemberLeadership member = event.getLeadershipRevision().getCurrent();
        return doFormat(
                "leadership",
                member.getMemberId(),
                "n/a",
                "n/a",
                "n/a",
                "n/a",
                member.getLeadershipState().name(),
                event.getLeadershipRevision().getRevision() + ""
        );
    }

    private static String doFormat(String eventType,
                                   String memberId,
                                   String memberActive,
                                   String memberRegistered,
                                   String memberEnabled,
                                   String memberRevision,
                                   String leadershipState,
                                   String leadershipRevision) {
        return String.format(
                "eventType=[%6s] memberId=%s active=%-5s registered=%-5s enabled=%-4s memberRevision=%-8s leadershipState=%-10s leadershipRevision=%s",
                eventType,
                memberId,
                memberActive,
                memberRegistered,
                memberEnabled,
                memberRevision,
                leadershipState,
                leadershipRevision
        );
    }
}
