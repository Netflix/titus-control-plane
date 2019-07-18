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

package com.netflix.titus.ext.k8s.clustermembership.connector;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipRevision;
import com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipRevisionResource;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.apis.CustomObjectsApi;
import io.kubernetes.client.models.V1DeleteOptionsBuilder;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.util.ClientBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipModelConverters.CRD_GROUP;
import static com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipModelConverters.CRD_PLURAL_MEMBERS;
import static com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipModelConverters.CRD_VERSION;
import static com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipModelConverters.toClusterMembershipRevision;
import static com.netflix.titus.ext.k8s.clustermembership.connector.model.K8ClusterMembershipModelConverters.toK8ClusterMembershipRevisionResource;

class DefaultK8MembershipExecutor implements K8MembershipExecutor {

    private final String namespace;

    private final ApiClient k8ApiClient;
    private final CustomObjectsApi k8CustomObjectClient;

    public DefaultK8MembershipExecutor(ApiClient k8ApiClient,
                                       String namespace) {
        this.namespace = namespace;
        this.k8ApiClient = k8ApiClient;
        this.k8CustomObjectClient = new CustomObjectsApi(k8ApiClient);
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> getMemberById(String memberId) {
        return K8ClientReactorAdapters.doGet(callbackHandler ->
                        k8CustomObjectClient.getNamespacedCustomObjectAsync(
                                CRD_GROUP,
                                CRD_VERSION,
                                namespace,
                                CRD_PLURAL_MEMBERS,
                                memberId,
                                callbackHandler
                        ),
                K8ClusterMembershipRevision.class
        ).map(k8Status -> {
            ClusterMembershipRevision revision = toClusterMembershipRevision((K8ClusterMembershipRevision) k8Status.getSpec());
            return applyNewRevisionNumber(revision, k8Status.getMetadata());
        });
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> createLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        // K8S will not accept create request if the version is set.
        ClusterMember currentWithoutVersion = localMemberRevision.getCurrent().toBuilder()
                .withLabels(CollectionsExt.copyAndRemove(localMemberRevision.getCurrent().getLabels(), "resourceVersion"))
                .build();
        ClusterMembershipRevision noVersionRevision = localMemberRevision.toBuilder().withCurrent(currentWithoutVersion).build();

        return K8ClientReactorAdapters.doUpdate(callbackHandler ->
                k8CustomObjectClient.createNamespacedCustomObjectAsync(
                        CRD_GROUP,
                        CRD_VERSION,
                        namespace,
                        CRD_PLURAL_MEMBERS,
                        toK8ClusterMembershipRevisionResource(namespace, noVersionRevision),
                        "",
                        callbackHandler
                )
        ).map(k8Status -> applyNewRevisionNumber(localMemberRevision, k8Status.getMetadata()));
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> updateLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        return K8ClientReactorAdapters.doUpdate(callbackHandler ->
                k8CustomObjectClient.replaceNamespacedCustomObjectAsync(
                        CRD_GROUP,
                        CRD_VERSION,
                        namespace,
                        CRD_PLURAL_MEMBERS,
                        localMemberRevision.getCurrent().getMemberId(),
                        toK8ClusterMembershipRevisionResource(namespace, localMemberRevision),
                        callbackHandler
                )
        ).map(k8Status -> applyNewRevisionNumber(localMemberRevision, k8Status.getMetadata()));
    }

    @Override
    public Mono<Void> removeLocal(String memberId) {
        return K8ClientReactorAdapters.doUpdate(callbackHandler ->
                k8CustomObjectClient.deleteNamespacedCustomObjectAsync(
                        CRD_GROUP,
                        CRD_VERSION,
                        namespace,
                        CRD_PLURAL_MEMBERS,
                        memberId,
                        new V1DeleteOptionsBuilder().build(),
                        0,
                        false,
                        null,
                        callbackHandler
                )
        ).ignoreElement().cast(Void.class);
    }

    @Override
    public Flux<ClusterMembershipEvent> watchMembershipEvents() {
        return K8ClientReactorAdapters.<K8ClusterMembershipRevisionResource>watch(
                k8ApiClient,
                () -> k8CustomObjectClient.listNamespacedCustomObjectCall(
                        CRD_GROUP,
                        CRD_VERSION,
                        namespace,
                        CRD_PLURAL_MEMBERS,
                        null,
                        null,
                        null,
                        null,
                        Integer.MAX_VALUE,
                        Boolean.TRUE,
                        null,
                        null
                ),
                K8ClientReactorAdapters.TYPE_WATCH_CLUSTER_MEMBERSHIP_REVISION_RESOURCE
        ).flatMap(event -> {
            K8ClusterMembershipRevisionResource revisionResource = event.object;
            ClusterMembershipRevision revision = applyNewRevisionNumber(
                    toClusterMembershipRevision(revisionResource.getSpec()),
                    revisionResource.getMetadata()
            );
            if (event.type.equals("ADDED")) {
                return Flux.just(ClusterMembershipEvent.memberAddedEvent(revision));
            }
            if (event.type.equals("DELETED")) {
                return Flux.just(ClusterMembershipEvent.memberRemovedEvent(revision));
            }
            if (event.type.equals("MODIFIED")) {
                return Flux.just(ClusterMembershipEvent.memberUpdatedEvent(revision));
            }
            return Flux.empty();
        });
    }

    private ClusterMembershipRevision<ClusterMember> applyNewRevisionNumber(ClusterMembershipRevision<ClusterMember> localMemberRevision, V1ObjectMeta metadata) {
        ClusterMember member = localMemberRevision.getCurrent();
        return localMemberRevision.toBuilder()
                .withRevision(metadata.getGeneration())
                .withCurrent(member.toBuilder()
                        .withLabels(CollectionsExt.copyAndAdd(member.getLabels(),
                                "resourceVersion", metadata.getResourceVersion()
                        ))
                        .build()
                )
                .build();
    }

    public static void main(String[] args) throws Exception {
        ApiClient client = ClientBuilder
                .standard()
                .setBasePath("http://100.65.82.159:7001")
                .build();
        client.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout
        DefaultK8MembershipExecutor executor = new DefaultK8MembershipExecutor(client, "default");

        ClusterMember member = ClusterMember.newBuilder()
                .withMemberId("member1")
                .withState(ClusterMemberState.Active)
                .withEnabled(true)
                .withClusterMemberAddresses(Collections.singletonList(
                        ClusterMemberAddress.newBuilder()
                                .withIpAddress("10.20.30.40")
                                .withPortNumber(8081)
                                .withProtocol("https")
                                .withSecure(true)
                                .withDescription("REST endpoint")
                                .build()
                ))
                .withLabels(Collections.singletonMap("resourceVersion", "1400519"))
                .build();
        ClusterMembershipRevision revision = ClusterMembershipRevision.newBuilder()
                .withCode("initial")
                .withMessage("Test")
                .withTimestamp(System.currentTimeMillis())
                .withCurrent(member)
                .build();

        Disposable watcherDisposable = executor.watchMembershipEvents().subscribe(
                System.out::println,
                Throwable::printStackTrace,
                () -> System.out.println("Watcher closed")
        );

        ClusterMembershipRevision<ClusterMember> result;
        try {
            result = executor.getMemberById("member1").block();
//            result = executor.createLocal(revision).block();
            result = executor.updateLocal(result.toBuilder().withTimestamp(System.currentTimeMillis()).build()).block();
//            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("SLEEP");
        Thread.sleep(60_000);

        System.exit(-1);
    }
}
