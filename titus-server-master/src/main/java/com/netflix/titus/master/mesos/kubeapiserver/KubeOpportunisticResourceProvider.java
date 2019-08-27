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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailabilityProvider;
import com.squareup.okhttp.Call;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CustomObjectsApi;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.util.CallGeneratorParams;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental(detail = "Informer-pattern based integration with k8s for opportunistic resources", deadline = "10/1/2019")
@Singleton
public class KubeOpportunisticResourceProvider implements OpportunisticCpuAvailabilityProvider {
    private static final Logger logger = LoggerFactory.getLogger(KubeOpportunisticResourceProvider.class);
    private static final ThreadGroup THREAD_GROUP = new ThreadGroup(KubeOpportunisticResourceProvider.class.getSimpleName());

    public static final String OPPORTUNISTIC_RESOURCE_GROUP = "titus.netflix.com";
    public static final String OPPORTUNISTIC_RESOURCE_VERSION = "v1";
    public static final String OPPORTUNISTIC_RESOURCE_NAMESPACE = "default";
    public static final String OPPORTUNISTIC_RESOURCE_SINGULAR = "opportunistic-resource";
    public static final String OPPORTUNISTIC_RESOURCE_PLURAL = "opportunistic-resources";

    private final CustomObjectsApi api;
    private final TitusRuntime titusRuntime;
    private final SharedIndexInformer<V1OpportunisticResource> informer;
    private final SharedInformerFactory informerFactory;

    @Inject
    public KubeOpportunisticResourceProvider(MesosConfiguration configuration, TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;
        ApiClient apiClient = Config.fromUrl(configuration.getKubeApiServerUrl());
        apiClient.getHttpClient().setReadTimeout(0, TimeUnit.SECONDS); // infinite timeout for watch calls
        // TODO(fabio): enhance the kube client to support custom ApiClient providers
        Configuration.setDefaultApiClient(apiClient);
        this.api = new CustomObjectsApi(apiClient);

        AtomicLong nextThreadNum = new AtomicLong(0);
        informerFactory = new SharedInformerFactory(Executors.newCachedThreadPool(runnable -> {
            Thread thread = new Thread(THREAD_GROUP, runnable,
                    THREAD_GROUP.getName() + "-" + nextThreadNum.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }));
        informer = informerFactory.sharedIndexInformerFor(
                listCallFactory(),
                V1OpportunisticResource.class,
                V1OpportunisticResourceList.class,
                configuration.getKubeOpportunisticRefreshIntervalMs()
        );
        informer.addEventHandler(new ResourceEventHandler<V1OpportunisticResource>() {
            @Override
            public void onAdd(V1OpportunisticResource resource) {
                logger.info("New opportunistic resources available: instance {}, expires at {}, cpus {}",
                        resource.getInstanceId(), resource.getEnd(), resource.getCpus());
            }

            @Override
            public void onUpdate(V1OpportunisticResource old, V1OpportunisticResource update) {
                logger.info("Opportunistic resources update: instance {}, expires at {}, cpus {}",
                        update.getInstanceId(), update.getEnd(), update.getCpus());
            }

            @Override
            public void onDelete(V1OpportunisticResource resource, boolean deletedFinalStateUnknown) {
                if (deletedFinalStateUnknown) {
                    logger.info("Stale opportunistic resource deleted, updates for it may have been missed in the past: instance {}, resource {}",
                            resource.getInstanceId(), resource.getEnd());
                } else {
                    logger.debug("Opportunistic resource GCed: instance {}, resource {}",
                            resource.getInstanceId(), resource.getUid());
                }
            }
        });

        // TODO(fabio): metrics on available opportunistic resources
    }

    @Activator
    public void enterActiveMode() {
        informerFactory.startAllRegisteredInformers();
    }

    public void shutdown() {
        informerFactory.stopAllRegisteredInformers();
    }

    private Function<CallGeneratorParams, Call> listCallFactory() {
        return params -> {
            try {
                return api.listNamespacedCustomObjectCall(
                        OPPORTUNISTIC_RESOURCE_GROUP,
                        OPPORTUNISTIC_RESOURCE_VERSION,
                        OPPORTUNISTIC_RESOURCE_NAMESPACE,
                        OPPORTUNISTIC_RESOURCE_PLURAL,
                        null,
                        null,
                        params.resourceVersion,
                        params.timeoutSeconds,
                        params.watch,
                        null,
                        null
                );
            } catch (ApiException e) {
                codeInvariants().unexpectedError("Error building a kube http call for opportunistic resources", e);
            }
            // this should never happen, if it does the code building request calls is wrong
            return null;
        };
    }

    private CodeInvariants codeInvariants() {
        return titusRuntime.getCodeInvariants();
    }

    private static final Comparator<OpportunisticCpuAvailability> EXPIRES_AT_COMPARATOR = Comparator
            .comparing(OpportunisticCpuAvailability::getExpiresAt);

    @Override
    public Map<String, OpportunisticCpuAvailability> getOpportunisticCpus() {
        return informer.getIndexer().list().stream().collect(Collectors.toMap(
                V1OpportunisticResource::getInstanceId,
                resource -> new OpportunisticCpuAvailability(resource.getUid(), resource.getEnd(), resource.getCpus()),
                BinaryOperator.maxBy(EXPIRES_AT_COMPARATOR)
        ));
    }
}
