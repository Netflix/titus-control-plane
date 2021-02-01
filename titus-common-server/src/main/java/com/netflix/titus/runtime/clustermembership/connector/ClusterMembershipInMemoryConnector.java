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

package com.netflix.titus.runtime.clustermembership.connector;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnector;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipSnapshotEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.rx.invoker.ReactorSerializedInvoker;
import com.netflix.titus.common.util.time.Clock;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@Singleton
public class ClusterMembershipInMemoryConnector implements ClusterMembershipConnector {

    private static final int MAX_TASK_QUEUE_SIZE = 10;
    private static final Duration EXCESSIVE_TASK_RUNNING_TIME = Duration.ofSeconds(1);

    private final Clock clock;
    private final AtomicLong revisionIdx = new AtomicLong();

    /**
     * Execute all actions in order of arrival.
     */
    private final ReactorSerializedInvoker<Object> taskInvoker;

    private volatile ClusterMembershipRevision<ClusterMember> localMemberRevision;
    private volatile ClusterMembershipRevision<ClusterMemberLeadership> localLeadershipRevision;

    private final DirectProcessor<ClusterMembershipEvent> eventProcessor = DirectProcessor.create();
    private final FluxSink<ClusterMembershipEvent> eventSink = eventProcessor.sink();

    @Inject
    public ClusterMembershipInMemoryConnector(ClusterMembershipRevision<ClusterMember> localMemberRevision,
                                              TitusRuntime titusRuntime,
                                              Scheduler scheduler) {
        this.localMemberRevision = localMemberRevision;
        this.clock = titusRuntime.getClock();

        this.localLeadershipRevision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(localMemberRevision.getCurrent().getMemberId())
                        .withLeadershipState(ClusterMemberLeadershipState.Disabled)
                        .build()
                )
                .withCode("started")
                .withMessage("Starts disabled after bootstrap")
                .withRevision(revisionIdx.incrementAndGet())
                .withTimestamp(clock.wallTime())
                .build();

        this.taskInvoker = ReactorSerializedInvoker.newBuilder()
                .withName(ClusterMembershipInMemoryConnector.class.getSimpleName())
                .withMaxQueueSize(MAX_TASK_QUEUE_SIZE)
                .withExcessiveRunningTime(EXCESSIVE_TASK_RUNNING_TIME)
                .withRegistry(titusRuntime.getRegistry())
                .withClock(titusRuntime.getClock())
                .withScheduler(scheduler)
                .build();
    }

    @PreDestroy
    public void shutdown() {
        taskInvoker.shutdown(EXCESSIVE_TASK_RUNNING_TIME);
        ReactorExt.safeDispose(eventProcessor);
    }

    @Override
    public ClusterMembershipRevision<ClusterMember> getLocalClusterMemberRevision() {
        return localMemberRevision;
    }

    @Override
    public Map<String, ClusterMembershipRevision<ClusterMember>> getClusterMemberSiblings() {
        return Collections.emptyMap();
    }

    @Override
    public ClusterMembershipRevision<ClusterMemberLeadership> getLocalLeadershipRevision() {
        return localLeadershipRevision;
    }

    @Override
    public Optional<ClusterMembershipRevision<ClusterMemberLeadership>> findCurrentLeader() {
        return localLeadershipRevision.getCurrent().getLeadershipState() == ClusterMemberLeadershipState.Leader
                ? Optional.of(localLeadershipRevision)
                : Optional.empty();
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> register(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        return invoke(Mono.fromCallable(() -> this.localMemberRevision = changeRegistrationStatus(selfUpdate.apply(localMemberRevision.getCurrent()), true)));
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> unregister(Function<ClusterMember, ClusterMembershipRevision<ClusterMember>> selfUpdate) {
        return invoke(Mono.fromCallable(() -> this.localMemberRevision = changeRegistrationStatus(selfUpdate.apply(localMemberRevision.getCurrent()), false)));
    }

    @Override
    public Mono<Void> joinLeadershipGroup() {
        return invoke(Mono.fromRunnable(() -> {
            if (changeLocalLeadershipState(ClusterMemberLeadershipState.Leader)) {
                eventProcessor.onNext(ClusterMembershipEvent.localJoinedElection(localLeadershipRevision));
            }
        }));
    }

    @Override
    public Mono<Boolean> leaveLeadershipGroup(boolean onlyNonLeader) {
        return invoke(Mono.fromCallable(() -> {
            if (inLeadershipState(ClusterMemberLeadershipState.Disabled) || onlyNonLeader) {
                return false;
            }
            changeLocalLeadershipState(ClusterMemberLeadershipState.Disabled);
            eventProcessor.onNext(ClusterMembershipEvent.leaderLost(localLeadershipRevision));
            return true;
        }));
    }

    @Override
    public Flux<ClusterMembershipEvent> membershipChangeEvents() {
        return eventProcessor.transformDeferred(ReactorExt.head(() -> Collections.singletonList(newSnapshot())));
    }

    private <T> Mono<T> invoke(Mono<T> action) {
        return taskInvoker.submit((Mono) action);
    }

    private boolean inLeadershipState(ClusterMemberLeadershipState state) {
        return localLeadershipRevision.getCurrent().getLeadershipState() == state;
    }

    private boolean changeLocalLeadershipState(ClusterMemberLeadershipState state) {
        if (!inLeadershipState(state)) {
            this.localLeadershipRevision = localLeadershipRevision.toBuilder()
                    .withCurrent(localLeadershipRevision.getCurrent().toBuilder().withLeadershipState(state).build())
                    .build();
            return true;
        }
        return false;
    }

    private ClusterMembershipRevision<ClusterMember> changeRegistrationStatus(ClusterMembershipRevision<ClusterMember> revision, boolean registered) {
        return revision.toBuilder()
                .withCurrent(revision.getCurrent().toBuilder().withRegistered(registered).build())
                .withRevision(revisionIdx.incrementAndGet())
                .withCode("change")
                .withMessage("Registration status change")
                .withTimestamp(clock.wallTime())
                .build();
    }

    private ClusterMembershipSnapshotEvent newSnapshot() {
        return ClusterMembershipEvent.snapshotEvent(
                Collections.singletonList(localMemberRevision),
                localLeadershipRevision,
                inLeadershipState(ClusterMemberLeadershipState.Leader) ? Optional.of(localLeadershipRevision) : Optional.empty()
        );
    }
}
