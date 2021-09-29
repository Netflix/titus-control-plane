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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.fabric8io;

import com.netflix.titus.api.clustermembership.connector.ClusterMembershipConnectorException;
import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import com.netflix.titus.api.clustermembership.model.event.ClusterMembershipEvent;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.ext.kube.clustermembership.connector.KubeMembershipExecutor;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.fabric8io.crd.FClusterMembership;
import com.netflix.titus.ext.kube.clustermembership.connector.transport.fabric8io.crd.Fabric8IOModelConverters;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class Fabric8IOKubeMembershipExecutor implements KubeMembershipExecutor {

    private final NamespacedKubernetesClient kubeApiClient;
    private final String namespace;

    private final NonNamespaceOperation<FClusterMembership, KubernetesResourceList<FClusterMembership>, Resource<FClusterMembership>> membershipClient;
    private final Scheduler scheduler;

    public Fabric8IOKubeMembershipExecutor(NamespacedKubernetesClient kubeApiClient,
                                           String namespace) {
        this.kubeApiClient = kubeApiClient;
        this.namespace = namespace;
        this.membershipClient = kubeApiClient.resources(FClusterMembership.class).inNamespace(namespace);
        this.scheduler = Schedulers.boundedElastic();
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> getMemberById(String memberId) {
        return Mono.fromCallable(() -> {
            FClusterMembership stored = membershipClient.withName(memberId).get();
            if (stored == null) {
                throw ClusterMembershipConnectorException.clientError("not found", null);
            }
            return applyNewRevisionNumber(
                    Fabric8IOModelConverters.toClusterMembershipRevision(stored.getSpec()),
                    stored.getMetadata()
            );
        }).subscribeOn(scheduler);
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> createLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        return Mono.fromCallable(() -> {
            // Kubernetes will not accept create request if the version is set.
            ClusterMember currentWithoutVersion = localMemberRevision.getCurrent().toBuilder()
                    .withLabels(CollectionsExt.copyAndRemove(localMemberRevision.getCurrent().getLabels(), "resourceVersion"))
                    .build();
            ClusterMembershipRevision<ClusterMember> noVersionRevision = localMemberRevision.toBuilder().withCurrent(currentWithoutVersion).build();

            FClusterMembership crd = Fabric8IOModelConverters.toCrdFClusterMembership(noVersionRevision);
            FClusterMembership storedCrd = membershipClient.create(crd);

            return applyNewRevisionNumber(
                    Fabric8IOModelConverters.toClusterMembershipRevision(storedCrd.getSpec()),
                    storedCrd.getMetadata()
            );
        }).subscribeOn(scheduler);
    }

    @Override
    public Mono<ClusterMembershipRevision<ClusterMember>> updateLocal(ClusterMembershipRevision<ClusterMember> localMemberRevision) {
        return Mono.fromCallable(() -> {
            FClusterMembership crd = Fabric8IOModelConverters.toCrdFClusterMembership(localMemberRevision);
            FClusterMembership storedCrd = membershipClient.withName(localMemberRevision.getCurrent().getMemberId()).replace(crd);
            return applyNewRevisionNumber(
                    Fabric8IOModelConverters.toClusterMembershipRevision(storedCrd.getSpec()),
                    storedCrd.getMetadata()
            );
        }).subscribeOn(scheduler);
    }

    @Override
    public Mono<Void> removeMember(String memberId) {
        return Mono.<Void>fromRunnable(() -> {
            membershipClient.withName(memberId).delete();
        }).subscribeOn(scheduler);
    }

    @Override
    public Flux<ClusterMembershipEvent> watchMembershipEvents() {
        return Flux.create(sink -> {
            membershipClient.watch(
                    new Watcher<FClusterMembership>() {
                        @Override
                        public void eventReceived(Action action, FClusterMembership resource) {
                            ClusterMembershipRevision<ClusterMember> revision = applyNewRevisionNumber(
                                    Fabric8IOModelConverters.toClusterMembershipRevision(resource.getSpec()),
                                    resource.getMetadata()
                            );
                            switch (action) {
                                case ADDED:
                                    sink.next(ClusterMembershipEvent.memberAddedEvent(revision));
                                    break;
                                case MODIFIED:
                                    sink.next(ClusterMembershipEvent.memberUpdatedEvent(revision));
                                    break;
                                case DELETED:
                                    sink.next(ClusterMembershipEvent.memberRemovedEvent(revision));
                                    break;
                                case ERROR:
                                    // Do nothing, as we close the flux below in onClose invocation
                            }
                        }

                        @Override
                        public void onClose(WatcherException cause) {
                            KubernetesClientException clientException = cause.asClientException();
                            sink.error(new IllegalStateException("Kubernetes watch stream error: " + clientException.toString()));
                        }
                    });
        });
    }

    private ClusterMembershipRevision<ClusterMember> applyNewRevisionNumber(ClusterMembershipRevision<ClusterMember> localMemberRevision, ObjectMeta metadata) {
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
