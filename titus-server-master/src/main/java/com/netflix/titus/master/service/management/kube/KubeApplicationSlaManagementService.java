/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.master.service.management.kube;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.kube.crd.F8IOCapacityGroup;
import com.netflix.titus.master.service.management.kube.crd.F8IOCapacityGroupSpec;
import com.netflix.titus.master.service.management.kube.crd.Fabric8IOModelConverters;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import rx.Observable;

@Singleton
public class KubeApplicationSlaManagementService implements ApplicationSlaManagementService {

    private static final Logger logger = LoggerFactory.getLogger(KubeApplicationSlaManagementService.class);

    private final CapacityGroupInformerWrapper capacityGroupInformer;
    private final NonNamespaceOperation<F8IOCapacityGroup, KubernetesResourceList<F8IOCapacityGroup>, Resource<F8IOCapacityGroup>> crdClient;
    private final Scheduler scheduler;

    private volatile Map<String, ApplicationSLA> capacityGroups = Collections.emptyMap();

    @Inject
    public KubeApplicationSlaManagementService(NamespacedKubernetesClient kubeApiClient, TitusRuntime titusRuntime) {
        this.crdClient = kubeApiClient.resources(F8IOCapacityGroup.class).inNamespace("default");
        this.capacityGroupInformer = new CapacityGroupInformerWrapper(kubeApiClient, titusRuntime);
        this.scheduler = Schedulers.boundedElastic();

        capacityGroupInformer.getInformer().addEventHandler(new ResourceEventHandler<F8IOCapacityGroup>() {
            @Override
            public void onAdd(F8IOCapacityGroup obj) {
                ApplicationSLA mapped = Fabric8IOModelConverters.toApplicationSLA(obj);
                String name = mapped.getAppName();
                if (!StringExt.isEmpty(name)) {
                    ApplicationSLA existing = capacityGroups.get(name);
                    if (existing == null || !existing.equals(mapped)) {
                        capacityGroups = CollectionsExt.copyAndAdd(capacityGroups, name, mapped);
                        logger.info("Added/Updated capacity group cache: {}", mapped);
                    }
                }
            }

            @Override
            public void onUpdate(F8IOCapacityGroup oldObj, F8IOCapacityGroup newObj) {
                onAdd(newObj);
            }

            @Override
            public void onDelete(F8IOCapacityGroup obj, boolean deletedFinalStateUnknown) {
                String name = obj.getMetadata().getName();
                if (!StringExt.isEmpty(name)) {
                    String originalName = Fabric8IOModelConverters.toApplicationSlaName(obj);
                    if (capacityGroups.containsKey(originalName)) {
                        capacityGroups = CollectionsExt.copyAndRemove(capacityGroups, originalName);
                        logger.info("Removed capacity group from cache: {}", originalName);
                    } else {
                        logger.warn("Request to remove unknown capacity group from cache: {}", originalName);
                    }
                }
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        capacityGroupInformer.shutdown();
        scheduler.dispose();
    }

    @Override
    public Collection<ApplicationSLA> getApplicationSLAs() {
        return capacityGroups.values();
    }

    @Override
    public Collection<ApplicationSLA> getApplicationSLAsForScheduler(String schedulerName) {
        List<ApplicationSLA> filtered = new ArrayList<>();
        capacityGroups.forEach((name, capacityGroup) -> {
            if (capacityGroup.getSchedulerName().equals(schedulerName)) {
                filtered.add(capacityGroup);
            }
        });
        return filtered;
    }

    @Override
    public Optional<ApplicationSLA> findApplicationSLA(String applicationName) {
        return Optional.ofNullable(capacityGroups.get(applicationName));
    }

    @Override
    public ApplicationSLA getApplicationSLA(String applicationName) {
        return capacityGroups.get(applicationName);
    }

    @Override
    public Observable<Void> addApplicationSLA(ApplicationSLA applicationSLA) {
        ObjectMeta meta = Fabric8IOModelConverters.toF8IOMetadata(applicationSLA);
        F8IOCapacityGroupSpec spec = Fabric8IOModelConverters.toF8IOCapacityGroupSpec(applicationSLA);

        Mono<Void> action = Mono.<Void>fromRunnable(() -> {
            // First try update in case the CRD exists
            F8IOCapacityGroup newCrd = new F8IOCapacityGroup();
            newCrd.setMetadata(meta);
            newCrd.setSpec(spec);
            try {
                crdClient.create(newCrd);
                logger.info("Created new capacity group: {}", applicationSLA);
                return;
            } catch (Exception error) {
                if (error instanceof KubernetesClientException) {
                    KubernetesClientException kerror = (KubernetesClientException) error;
                    if (kerror.getCode() != 409) {
                        throw TitusServiceException.internal(error, "Cannot create/update capacity group: %s", applicationSLA);
                    }
                }
            }

            // We failed, so try again and create it.
            try {
                String name = meta.getName();
                F8IOCapacityGroup existingCrd = crdClient.withName(name).get();
                existingCrd.setSpec(spec);
                crdClient.withName(name).replace(existingCrd);
                logger.info("Updated an existing capacity group: {}", applicationSLA);
            } catch (Exception error) {
                throw TitusServiceException.internal(error, "Cannot create/update capacity group: %s", applicationSLA);
            }
        }).subscribeOn(scheduler);
        return ReactorExt.toObservable(action);
    }

    @Override
    public Observable<Void> removeApplicationSLA(String originalName) {
        String name = Fabric8IOModelConverters.toValidKubeCrdName(originalName);
        Mono<Void> action = Mono.<Void>fromRunnable(() -> {
            try {
                crdClient.withName(name).delete();
                logger.info("Removed a capacity group: {}", originalName);
            } catch (Exception error) {
                throw TitusServiceException.internal(error, "Cannot remove capacity group: %s", originalName);
            }
        }).subscribeOn(scheduler);
        return ReactorExt.toObservable(action);
    }
}
