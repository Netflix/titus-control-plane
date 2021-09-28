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

package com.netflix.titus.ext.kube.clustermembership.connector;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadership;
import com.netflix.titus.api.clustermembership.model.ClusterMemberLeadershipState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.jayway.awaitility.Awaitility.await;

class StubbedKubeExecutors implements KubeMembershipExecutor, KubeLeaderElectionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(StubbedKubeExecutors.class);

    private final String localMemberId;
    private final AtomicInteger memberRevisionIdx = new AtomicInteger();
    private final AtomicInteger leadershipRevisionIdx = new AtomicInteger();

    private volatile ClusterMembershipRevision<ClusterMember> localMemberRevision;
    private volatile ClusterMembershipRevision<ClusterMemberLeadership> localLeadershipRevision;
    private volatile boolean firstRegistration = true;
    private volatile boolean inLeaderElectionProcess;
    private volatile boolean localLeader;

    private volatile RuntimeException onMembershipUpdateError;
    private volatile int onMembershipUpdateErrorCount;

    private final ConcurrentMap<String, ClusterMembershipRevision<ClusterMember>> siblings = new ConcurrentHashMap<>();

    private volatile DirectProcessor<ClusterMembershipEvent> membershipEventsProcessor = DirectProcessor.create();
    private volatile DirectProcessor<ClusterMembershipEvent> leadershipEventsProcessor = DirectProcessor.create();

    StubbedKubeExecutors(String localMemberId) {
        this.localMemberId = localMemberId;
    }

    @Override
    public boolean isInLeaderElectionProcess() {
        return inLeaderElectionProcess;
    }

    @Override
    public boolean isLeader() {
        return localLeader;
    }

    @Override
    public boolean joinLeaderElectionProcess() {
        if (inLeaderElectionProcess) {
            return false;
        }
        this.inLeaderElectionProcess = true;
        return true;
    }

    @Override
    public void leaveLeaderElectionProcess() {
        if (!inLeaderElectionProcess) {
            return;
        }

        this.localLeadershipRevision = ClusterMembershipRevision.<ClusterMemberLeadership>newBuilder()
                .withCurrent(ClusterMemberLeadership.newBuilder()
                        .withMemberId(localMemberId)
                        .withLeadershipState(ClusterMemberLeadershipState.Disabled)
                        .build()
                )
                .withCode("testing")
                .withMessage("Called: leaveLeaderElectionProcess")
                .withRevision(leadershipRevisionIdx.getAndIncrement())
                .withTimestamp(System.currentTimeMillis())
                .build();

        if (localLeader) {
            localLeader = false;
            membershipEventsProcessor.onNext(ClusterMembershipEvent.leaderLost(localLeadershipRevision));
        } else {
            membershipEventsProcessor.onNext(ClusterMembershipEvent.localLeftElection(localLeadershipRevision));
        }
    }

    @Override
    public Flux<ClusterMembershipEvent> watchLeaderElectionProcessUpdates() {
        return Flux.defer(() -> {
            logger.info("Resubscribing to the leader election event stream...");
            return leadershipEventsProcessor;
        });
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> getMemberById(String memberId) {
        if (memberId.equals(this.localMemberId)) {
            return Mono.just(localMemberRevision);
        }
        ClusterMembershipRevision<ClusterMember> sibling = siblings.get(memberId);
        return sibling == null
                ? Mono.error(new IllegalArgumentException("Sibling not found: " + memberId))
                : Mono.just(sibling);
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> createLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        return handleMemberUpdate(() -> {
            Preconditions.checkState(firstRegistration, "Use updateLocal for the updates");
            this.localMemberRevision = localMemberRevision.toBuilder().withRevision(memberRevisionIdx.getAndIncrement()).build();
            this.firstRegistration = false;
            return Mono.just(this.localMemberRevision);
        });
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> updateLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        return handleMemberUpdate(() -> {
            Preconditions.checkState(!firstRegistration, "Use createLocal for the first time");
            this.localMemberRevision = localMemberRevision.toBuilder().withRevision(memberRevisionIdx.getAndIncrement()).build();
            return Mono.just(this.localMemberRevision);
        });
    }

    @Override
    public Mono<Void> removeMember(String memberId) {
        return handleMemberUpdate(() -> {
            if (memberId.equals(this.localMemberId)) {
                this.firstRegistration = true;
            } else {
                ClusterMembershipRevision<ClusterMember> removed = siblings.remove(memberId);
                membershipEventsProcessor.onNext(ClusterMembershipEvent.memberRemovedEvent(removed));
            }
            return Mono.empty();
        });
    }

    private <T> Mono<T> handleMemberUpdate(Supplier<Mono<T>> supplier) {
        return Mono.defer(() -> {
            if (onMembershipUpdateError != null && onMembershipUpdateErrorCount > 0) {
                onMembershipUpdateErrorCount--;
                return Mono.error(onMembershipUpdateError);
            }
            return supplier.get();
        });
    }

    @Override
    public Flux<ClusterMembershipEvent> watchMembershipEvents() {
        return Flux.defer(() -> {
            logger.info("Resubscribing to the membership event stream...");
            return membershipEventsProcessor;
        });
    }

    void breakMembershipEventSource() {
        membershipEventsProcessor.onError(new RuntimeException("Simulated membership watch error"));
        membershipEventsProcessor = DirectProcessor.create();
        await().until(() -> membershipEventsProcessor.hasDownstreams());
    }

    void completeMembershipEventSource() {
        membershipEventsProcessor.onComplete();
        membershipEventsProcessor = DirectProcessor.create();
        await().until(() -> membershipEventsProcessor.hasDownstreams());
    }

    void addOrUpdateSibling(ClusterMembershipRevision<ClusterMember> siblingRevision) {
        String siblingId = siblingRevision.getCurrent().getMemberId();
        boolean update = siblings.containsKey(siblingId);

        siblings.put(siblingId, siblingRevision);

        membershipEventsProcessor.onNext(update
                ? ClusterMembershipEvent.memberUpdatedEvent(siblingRevision)
                : ClusterMembershipEvent.memberAddedEvent(siblingRevision)
        );
    }

    void removeSibling(String siblingId) {
        if (!siblings.containsKey(siblingId)) {
            return;
        }

        ClusterMembershipRevision<ClusterMember> removed = siblings.remove(siblingId);
        membershipEventsProcessor.onNext(ClusterMembershipEvent.memberRemovedEvent(removed));
    }

    boolean isFailingOnMembershipUpdate() {
        return onMembershipUpdateErrorCount > 0;
    }

    void failOnMembershipUpdate(RuntimeException cause, int count) {
        onMembershipUpdateError = cause;
        onMembershipUpdateErrorCount = count;
    }

    void doNotFailOnMembershipUpdate() {
        onMembershipUpdateErrorCount = 0;
    }

    void emitLeadershipEvent(LeaderElectionChangeEvent event) {
        if (event.getChangeType() == LeaderElectionChangeEvent.ChangeType.LeaderElected) {
            if (event.getLeadershipRevision().getCurrent().getMemberId().equals(localMemberId)) {
                localLeader = true;
                localLeadershipRevision = event.getLeadershipRevision();
            } else {
                localLeader = false;
            }
        }
        leadershipEventsProcessor.onNext(event);
    }

    void breakLeadershipEventSource() {
        leadershipEventsProcessor.onError(new RuntimeException("Simulated leadership watch error"));
        leadershipEventsProcessor = DirectProcessor.create();
        await().until(() -> leadershipEventsProcessor.hasDownstreams());
    }

    void completeLeadershipEventSource() {
        leadershipEventsProcessor.onComplete();
        leadershipEventsProcessor = DirectProcessor.create();
        await().until(() -> leadershipEventsProcessor.hasDownstreams());
    }
}
