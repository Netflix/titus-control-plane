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

package com.netflix.titus.runtime.clustermembership.endpoint.grpc;

import java.time.Duration;
import java.util.function.Consumer;

import com.netflix.titus.api.clustermembership.service.ClusterMembershipService;
import com.netflix.titus.grpc.protogen.ClusterMember;
import com.netflix.titus.grpc.protogen.ClusterMembershipEvent;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevision;
import com.netflix.titus.grpc.protogen.ClusterMembershipRevisions;
import com.netflix.titus.grpc.protogen.DeleteMemberLabelsRequest;
import com.netflix.titus.grpc.protogen.EnableMemberRequest;
import com.netflix.titus.grpc.protogen.MemberId;
import com.netflix.titus.grpc.protogen.UpdateMemberLabelsRequest;
import com.netflix.titus.runtime.clustermembership.client.ReactorClusterMembershipClient;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipServiceStub.LOCAL_MEMBER;
import static com.netflix.titus.runtime.clustermembership.endpoint.grpc.ClusterMembershipServiceStub.LOCAL_MEMBER_ID;
import static org.assertj.core.api.Assertions.assertThat;

public class ClusterMembershipGrpcServerTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final ClusterMembershipService service = new ClusterMembershipServiceStub();

    @Rule
    public final ClusterMembershipServerResource serverResource = new ClusterMembershipServerResource(service);

    private final TitusRxSubscriber<ClusterMembershipEvent> eventSubscriber = new TitusRxSubscriber<>();

    private ReactorClusterMembershipClient client;

    @Before
    public void setUp() {
        this.client = serverResource.getClient();
        client.events().subscribe(eventSubscriber);
    }

    @Test(timeout = 30_000)
    public void testGetMember() {
        ClusterMembershipRevision result = client.getMember(MemberId.newBuilder().setId("local").build()).block();
        assertThat(result).isNotNull();
        assertThat(result.getCurrent().getMemberId()).isEqualTo(LOCAL_MEMBER.getCurrent().getMemberId());
    }

    @Test(timeout = 30_000)
    public void testGetMembers() {
        ClusterMembershipRevisions result = client.getMembers().block();
        assertThat(result).isNotNull();
        assertThat(result.getRevisionsCount()).isEqualTo(3);
    }

    @Test(timeout = 30_000)
    public void testUpdateAndRemoveLabel() throws InterruptedException {
        checkSnapshotEvent();

        // Add label
        ClusterMembershipRevision addResult = client.updateMemberLabels(UpdateMemberLabelsRequest.newBuilder()
                .setMemberId(LOCAL_MEMBER_ID)
                .putLabels("keyA", "valueA")
                .build()
        ).block();

        assertThat(addResult).isNotNull();
        assertThat(addResult.getCurrent().getLabelsMap()).containsEntry("keyA", "valueA");
        expectMemberUpdateEvent(update -> assertThat(update.getLabelsMap()).containsEntry("keyA", "valueA"));

        // Add label
        ClusterMembershipRevision removeResult = client.deleteMemberLabels(DeleteMemberLabelsRequest.newBuilder()
                .setMemberId(LOCAL_MEMBER_ID)
                .addKeys("keyA")
                .build()
        ).block();

        assertThat(removeResult).isNotNull();
        assertThat(removeResult.getCurrent().getLabelsMap()).doesNotContainKeys("keyA");
        expectMemberUpdateEvent(update -> assertThat(update.getLabelsMap()).doesNotContainKeys("keyA"));
    }

    @Test
    public void testEnableLocalMember() throws InterruptedException {
        checkSnapshotEvent();

        // Started enabled, so disable first
        ClusterMembershipRevision disabledResult = client.enableMember(
                EnableMemberRequest.newBuilder().setMemberId(LOCAL_MEMBER_ID).setEnabled(false).build()
        ).block();
        assertThat(disabledResult.getCurrent().getEnabled()).isFalse();
        expectMemberUpdateEvent(update -> assertThat(update.getEnabled()).isFalse());

        // Now enable again
        ClusterMembershipRevision enabledResult = client.enableMember(
                EnableMemberRequest.newBuilder().setMemberId(LOCAL_MEMBER_ID).setEnabled(true).build()
        ).block();
        assertThat(enabledResult.getCurrent().getEnabled()).isTrue();
        expectMemberUpdateEvent(update -> assertThat(update.getEnabled()).isTrue());
    }

    @Test
    public void testLeaderFailover() throws InterruptedException {
        checkSnapshotEvent();

        client.stopBeingLeader().block();
        expectMemberUpdateEvent(local -> {
            assertThat(local.getMemberId()).isEqualTo(LOCAL_MEMBER_ID);
            assertThat(local.getLeadershipState()).isEqualTo(ClusterMember.LeadershipState.NonLeader);
        });
        expectMemberUpdateEvent(sibling -> {
            assertThat(sibling.getMemberId()).startsWith("sibling");
            assertThat(sibling.getLeadershipState()).isEqualTo(ClusterMember.LeadershipState.Leader);
        });
    }

    private void checkSnapshotEvent() throws InterruptedException {
        ClusterMembershipEvent event = eventSubscriber.takeNext(TIMEOUT);
        assertThat(event.getEventCase()).isEqualTo(ClusterMembershipEvent.EventCase.SNAPSHOT);
    }

    private void expectMemberUpdateEvent(Consumer<ClusterMember> assertion) throws InterruptedException {
        ClusterMembershipEvent event = eventSubscriber.takeNext(TIMEOUT);
        assertThat(event.getEventCase()).isEqualTo(ClusterMembershipEvent.EventCase.MEMBERUPDATED);
        assertion.accept(event.getMemberUpdated().getRevision().getCurrent());
    }
}