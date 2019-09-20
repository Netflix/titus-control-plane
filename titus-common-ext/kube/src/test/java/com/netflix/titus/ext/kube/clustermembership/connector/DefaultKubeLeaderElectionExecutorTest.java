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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.clustermembership.model.event.LeaderElectionChangeEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.junit.category.RemoteIntegrationTest;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import io.kubernetes.client.ApiClient;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

@Category(RemoteIntegrationTest.class)
public class DefaultKubeLeaderElectionExecutorTest {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKubeLeaderElectionExecutor.class);

    @ClassRule
    public static final KubeExternalResource KUBE_RESOURCE = new KubeExternalResource();

    private static final Duration LEASE_DURATION = Duration.ofSeconds(1);

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final String clusterName = newClusterId();

    private final List<MemberHolder> memberHolders = new ArrayList<>();

    @After
    public void tearDown() {
        memberHolders.forEach(MemberHolder::close);
    }

    @Test
    public void testLeaderElection() {
        // Join first member
        MemberHolder member1 = new MemberHolder(KUBE_RESOURCE.getClient());
        joinLeaderElectionProcess(member1);
        awaitBeingLeader(member1);

        LeaderElectionChangeEvent leaderSelectedEvent = member1.takeNextEvent();
        assertThat(leaderSelectedEvent.getLeadershipRevision().getCurrent().getMemberId()).isEqualTo(member1.getMemberId());
        assertThat(leaderSelectedEvent.getChangeType()).isEqualTo(LeaderElectionChangeEvent.ChangeType.LeaderElected);

        // Join second member
        MemberHolder member2 = new MemberHolder(KUBE_RESOURCE.getClient());
        joinLeaderElectionProcess(member2);

        // Leave leader election process.
        member1.getExecutor().leaveLeaderElectionProcess();
        await().until(() -> {
            LeaderElectionChangeEvent event = member1.takeNextEvent();
            return event != null && event.getChangeType() == LeaderElectionChangeEvent.ChangeType.LeaderLost;
        });

        // Check that second member takes over leadership
        awaitBeingLeader(member2);
    }

    private void joinLeaderElectionProcess(MemberHolder member) {
        assertThat(member.getExecutor().joinLeaderElectionProcess()).isTrue();
        assertThat(member.getExecutor().isInLeaderElectionProcess()).isTrue();
    }

    private void awaitBeingLeader(MemberHolder member) {
        await().until(() -> member.getExecutor().isLeader());
    }

    private String newClusterId() {
        return "junit-cluster-" + System.getenv("USER") + "-" + System.currentTimeMillis();
    }

    private String newMemberId() {
        return "junit-member-" + System.getenv("USER") + "-" + System.currentTimeMillis();
    }

    private class MemberHolder {

        private final String memberId;
        private final DefaultKubeLeaderElectionExecutor executor;
        private final TitusRxSubscriber<LeaderElectionChangeEvent> eventSubscriber = new TitusRxSubscriber<>();

        MemberHolder(ApiClient client) {
            this.memberId = newMemberId();
            this.executor = new DefaultKubeLeaderElectionExecutor(
                    client,
                    "default",
                    clusterName,
                    LEASE_DURATION,
                    memberId,
                    titusRuntime
            );
            memberHolders.add(this);

            executor.watchLeaderElectionProcessUpdates()
                    .map(event -> {
                        logger.info("[{}] Event stream update: {}", memberId, event);
                        if (event instanceof LeaderElectionChangeEvent) {
                            return event;
                        }
                        throw new IllegalStateException("Unexpected event in the stream: " + event);
                    })
                    .cast(LeaderElectionChangeEvent.class)
                    .subscribe(eventSubscriber);

            assertThat(executor.isInLeaderElectionProcess()).isFalse();
        }

        String getMemberId() {
            return memberId;
        }

        DefaultKubeLeaderElectionExecutor getExecutor() {
            return executor;
        }

        LeaderElectionChangeEvent takeNextEvent() {
            try {
                return Preconditions.checkNotNull(eventSubscriber.takeNext(KubeExternalResource.KUBE_TIMEOUT));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        void close() {
            executor.leaveLeaderElectionProcess();
        }
    }
}