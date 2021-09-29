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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.main;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeMembershipExecutor;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.main.crd.KubeClusterMembershipModelConverters;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.main.crd.KubeClusterMembershipRevision;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.main.crd.KubeClusterMembershipRevisionResource;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1DeleteOptionsBuilder;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MainKubeMembershipExecutor implements KubeMembershipExecutor {

    private final String namespace;

    private final ApiClient kubeApiClient;
    private final CustomObjectsApi kubeCustomObjectClient;

    public MainKubeMembershipExecutor(ApiClient kubeApiClient,
                                      String namespace) {
        this.namespace = namespace;
        this.kubeApiClient = kubeApiClient;
        this.kubeCustomObjectClient = new CustomObjectsApi(kubeApiClient);
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> getMemberById(String memberId) {
        return KubeClientReactorAdapters.doGet(callbackHandler ->
                        kubeCustomObjectClient.getNamespacedCustomObjectAsync(
                                KubeClusterMembershipModelConverters.CRD_GROUP,
                                KubeClusterMembershipModelConverters.CRD_VERSION,
                                namespace,
                                KubeClusterMembershipModelConverters.CRD_PLURAL_MEMBERS,
                                memberId,
                                callbackHandler
                        ),
                KubeClusterMembershipRevision.class
        ).map(kubeStatus -> {
            ClusterMembershipRevision revision = KubeClusterMembershipModelConverters.toClusterMembershipRevision((KubeClusterMembershipRevision) kubeStatus.getSpec());
            return applyNewRevisionNumber(revision, kubeStatus.getMetadata());
        });
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> createLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        // Kubernetes will not accept create request if the version is set.
        ClusterMember currentWithoutVersion = localMemberRevision.getCurrent().toBuilder()
                .withLabels(CollectionsExt.copyAndRemove(localMemberRevision.getCurrent().getLabels(), "resourceVersion"))
                .build();
        ClusterMembershipRevision noVersionRevision = localMemberRevision.toBuilder().withCurrent(currentWithoutVersion).build();

        return KubeClientReactorAdapters.doUpdate(callbackHandler ->
                kubeCustomObjectClient.createNamespacedCustomObjectAsync(
                        KubeClusterMembershipModelConverters.CRD_GROUP,
                        KubeClusterMembershipModelConverters.CRD_VERSION,
                        namespace,
                        KubeClusterMembershipModelConverters.CRD_PLURAL_MEMBERS,
                        KubeClusterMembershipModelConverters.toKubeClusterMembershipRevisionResource(namespace, noVersionRevision),
                        "",
                        null,
                        null,
                        callbackHandler
                )
        ).map(kubeStatus -> applyNewRevisionNumber(localMemberRevision, kubeStatus.getMetadata()));
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> updateLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        return KubeClientReactorAdapters.doUpdate(callbackHandler ->
                kubeCustomObjectClient.replaceNamespacedCustomObjectAsync(
                        KubeClusterMembershipModelConverters.CRD_GROUP,
                        KubeClusterMembershipModelConverters.CRD_VERSION,
                        namespace,
                        KubeClusterMembershipModelConverters.CRD_PLURAL_MEMBERS,
                        localMemberRevision.getCurrent().getMemberId(),
                        KubeClusterMembershipModelConverters.toKubeClusterMembershipRevisionResource(namespace, localMemberRevision),
                        null,
                        null,
                        callbackHandler
                )
        ).map(kubeStatus -> applyNewRevisionNumber(localMemberRevision, kubeStatus.getMetadata()));
    }

    @Override
    public Mono<Void> removeMember(String memberId) {
        return KubeClientReactorAdapters.doUpdate(callbackHandler ->
                kubeCustomObjectClient.deleteNamespacedCustomObjectAsync(
                        KubeClusterMembershipModelConverters.CRD_GROUP,
                        KubeClusterMembershipModelConverters.CRD_VERSION,
                        namespace,
                        KubeClusterMembershipModelConverters.CRD_PLURAL_MEMBERS,
                        memberId,
                        0,
                        false,
                        null,
                        null,
                        new V1DeleteOptionsBuilder().build(),
                        callbackHandler
                )
        ).ignoreElement().cast(Void.class);
    }

    @Override
    public Flux<ClusterMembershipEvent> watchMembershipEvents() {
        return KubeClientReactorAdapters.<KubeClusterMembershipRevisionResource>watch(
                kubeApiClient,
                () -> kubeCustomObjectClient.listNamespacedCustomObjectCall(
                        KubeClusterMembershipModelConverters.CRD_GROUP,
                        KubeClusterMembershipModelConverters.CRD_VERSION,
                        namespace,
                        KubeClusterMembershipModelConverters.CRD_PLURAL_MEMBERS,
                        null,
                        null,
                        null,
                        null,
                        Integer.MAX_VALUE,
                        null,
                        null,
                        Boolean.TRUE,
                        null
                ),
                KubeClientReactorAdapters.TYPE_WATCH_CLUSTER_MEMBERSHIP_REVISION_RESOURCE
        ).flatMap(event -> {
            KubeClusterMembershipRevisionResource revisionResource = event.object;
            ClusterMembershipRevision revision = applyNewRevisionNumber(
                    KubeClusterMembershipModelConverters.toClusterMembershipRevision(revisionResource.getSpec()),
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
            if (event.type.equals("ERROR")) {
                String message = event.status != null
                        ? "Kubernetes watch stream error: " + event.status.toString()
                        : "Kubernetes watch stream error (no details provided)";
                return Mono.error(new IllegalStateException(message));
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
}
