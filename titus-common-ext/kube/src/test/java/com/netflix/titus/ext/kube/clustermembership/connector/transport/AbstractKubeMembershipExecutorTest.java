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

package com.netflix.titus.ext.kube.clustermembership.connector.transport;

import java.util.ArrayList;
import java.util.List;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnectorException;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipChangeEvent;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeMembershipExecutor;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.main.MainKubeExternalResource;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.activeClusterMember;
import static com.netflix.titus.testkit.model.clustermembership.ClusterMemberGenerator.clusterMemberRegistrationRevision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public abstract class AbstractKubeMembershipExecutorTest {

    private final TitusRxSubscriber<ClusterMembershipEvent> eventSubscriber = new TitusRxSubscriber<>();

    private final List<String> createdMemberIds = new ArrayList<>();

    private KubeMembershipExecutor executor;

    @Before
    public void setUp() {
        this.executor = getExecutor();
        executor.watchMembershipEvents().subscribe(eventSubscriber);
    }

    @After
    public void tearDown() {
        ReactorExt.safeDispose(eventSubscriber);
        createdMemberIds.forEach(memberId -> ExceptionExt.silent(() -> executor.removeMember(memberId).block(MainKubeExternalResource.KUBE_TIMEOUT)));
    }

    protected abstract KubeMembershipExecutor getExecutor();

    @Test(timeout = 30_000)
    public void testCreateAndGetNewMember() throws InterruptedException {
        ClusterMembershipRevision<ClusterMember> revision = newMemberRevision();
        String memberId = revision.getCurrent().getMemberId();

        ClusterMembershipRevision<ClusterMember> result = executor.createLocal(revision).block();

        assertThat(result).isNotNull();
        assertThat(result.getCurrent().getMemberId()).isEqualTo(memberId);
        assertThat(result.getRevision()).isNotNull();

        expectClusterMembershipChangeEvent(result, ClusterMembershipChangeEvent.ChangeType.Added);

        // Now get it
        ClusterMembershipRevision<ClusterMember> fetched = executor.getMemberById(memberId).block();
        assertThat(fetched).isEqualTo(result);
    }

    @Test(timeout = 30_000)
    public void testUpdateExistingMember() throws InterruptedException {
        ClusterMembershipRevision<ClusterMember> initialRevision = newMemberRevision();
        ClusterMembershipRevision<ClusterMember> initialResponse = executor.createLocal(initialRevision).block();

        ClusterMembershipRevision<ClusterMember> secondResponse = executor.updateLocal(initialResponse.toBuilder().withCode("update").build()).block();
        assertThat(secondResponse).isNotNull();
        assertThat(secondResponse.getCode()).isEqualTo("update");

        expectClusterMembershipChangeEvent(initialResponse, ClusterMembershipChangeEvent.ChangeType.Added);
        expectClusterMembershipChangeEvent(secondResponse, ClusterMembershipChangeEvent.ChangeType.Updated);
    }

    @Test
    public void testRemoveMember() throws InterruptedException {
        ClusterMembershipRevision<ClusterMember> revision = newMemberRevision();
        String memberId = revision.getCurrent().getMemberId();

        ClusterMembershipRevision<ClusterMember> result = executor.createLocal(revision).block();
        executor.removeMember(memberId).block();
        try {
            ClusterMembershipRevision<ClusterMember> retrieved = executor.getMemberById(memberId).block();
            fail("Found removed member: {}", retrieved);
        } catch (ClusterMembershipConnectorException e) {
            assertThat(e.getMessage()).contains("not found");
        } catch (Exception e) {
            assertThat(KubeUtils.is4xx(e)).isTrue();
        }

        expectClusterMembershipChangeEvent(result, ClusterMembershipChangeEvent.ChangeType.Added);
        assertThat(findNextMemberEvent(memberId)).isNotNull();
    }

    private void expectClusterMembershipChangeEvent(ClusterMembershipRevision<ClusterMember> revision,
                                                    ClusterMembershipChangeEvent.ChangeType changeType) throws InterruptedException {
        ClusterMembershipChangeEvent memberEvent = findNextMemberEvent(revision.getCurrent().getMemberId());
        assertThat(memberEvent.getChangeType()).isEqualTo(changeType);
        assertThat(memberEvent.getRevision()).isEqualTo(revision);
    }

    private ClusterMembershipChangeEvent findNextMemberEvent(String memberId) throws InterruptedException {
        while (true) {
            ClusterMembershipEvent event = eventSubscriber.takeNext(MainKubeExternalResource.KUBE_TIMEOUT);
            assertThat(event).isNotNull();
            if (event instanceof ClusterMembershipChangeEvent) {
                ClusterMembershipChangeEvent memberEvent = (ClusterMembershipChangeEvent) event;
                if (memberEvent.getRevision().getCurrent().getMemberId().equals(memberId)) {
                    return memberEvent;
                }
            }
        }
    }

    private String newMemberId() {
        String memberId = "junit-member-" + System.getenv("USER") + "-" + System.currentTimeMillis();
        createdMemberIds.add(memberId);
        return memberId;
    }

    private ClusterMembershipRevision<ClusterMember> newMemberRevision() {
        return clusterMemberRegistrationRevision(activeClusterMember(newMemberId()).toBuilder().withRegistered(false).build());
    }
}
