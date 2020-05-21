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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.master.mesos.kubeapiserver.client.KubeApiFacade;
import com.netflix.titus.master.mesos.kubeapiserver.model.v1.V1OpportunisticResource;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailability;
import com.netflix.titus.master.scheduler.opportunistic.OpportunisticCpuAvailabilityProvider;
import io.kubernetes.client.informer.ResourceEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental(detail = "Informer-pattern based integration with Kubernetes for opportunistic resources", deadline = "12/1/2019")
@Singleton
public class KubeOpportunisticResourceProvider implements OpportunisticCpuAvailabilityProvider {

    private static final Logger logger = LoggerFactory.getLogger(KubeOpportunisticResourceProvider.class);

    private final KubeApiFacade kubeApiFacade;
    private final TitusRuntime titusRuntime;
    private final Id cpuSupplyId;

    private ScheduledExecutorService metricsPollerExecutor;

    private static long currentOpportunisticCpuCount(KubeOpportunisticResourceProvider self) {
        return self.getOpportunisticCpus().values().stream()
                .filter(self::isNotExpired)
                .mapToLong(OpportunisticCpuAvailability::getCount)
                .sum();
    }

    @Inject
    public KubeOpportunisticResourceProvider(KubeApiFacade kubeApiFacade, TitusRuntime titusRuntime) {
        this.kubeApiFacade = kubeApiFacade;
        this.titusRuntime = titusRuntime;
        cpuSupplyId = titusRuntime.getRegistry().createId("titusMaster.opportunistic.supply.cpu");

    }

    @Activator
    public void enterActiveMode() {
        this.metricsPollerExecutor = ExecutorsExt.namedSingleThreadScheduledExecutor("kube-opportunistic-cpu-metrics-poller");
        PolledMeter.using(titusRuntime.getRegistry())
                .withId(cpuSupplyId)
                .scheduleOn(metricsPollerExecutor)
                .monitorValue(this, KubeOpportunisticResourceProvider::currentOpportunisticCpuCount);

        // TODO(fabio): metrics on available opportunistic resources
        kubeApiFacade.getOpportunisticResourceInformer().addEventHandler(new ResourceEventHandler<V1OpportunisticResource>() {
            @Override
            public void onAdd(V1OpportunisticResource resource) {
                logger.info("New opportunistic resources available: instance {}, expires at {}, cpus {}, name {}",
                        resource.getInstanceId(), resource.getEnd(), resource.getCpus(), resource.getName());
            }

            @Override
            public void onUpdate(V1OpportunisticResource old, V1OpportunisticResource update) {
                logger.info("Opportunistic resources update: instance {}, expires at {}, cpus {}, name {}",
                        update.getInstanceId(), update.getEnd(), update.getCpus(), update.getName());
            }

            @Override
            public void onDelete(V1OpportunisticResource resource, boolean deletedFinalStateUnknown) {
                if (deletedFinalStateUnknown) {
                    logger.info("Stale opportunistic resource deleted, updates for it may have been missed in the past: instance {}, resource {}, name {}",
                            resource.getInstanceId(), resource.getUid(), resource.getName());
                } else {
                    logger.debug("Opportunistic resource GCed: instance {}, resource {}, name {}",
                            resource.getInstanceId(), resource.getUid(), resource.getName());
                }
            }
        });
    }

    @Deactivator
    public void shutdown() {
        PolledMeter.remove(titusRuntime.getRegistry(), cpuSupplyId);
        Evaluators.acceptNotNull(metricsPollerExecutor, ExecutorService::shutdown);
    }

    private static final Comparator<OpportunisticCpuAvailability> EXPIRES_AT_COMPARATOR = Comparator
            .comparing(OpportunisticCpuAvailability::getExpiresAt);

    /**
     * Assumptions:
     * <ul>
     *     <li>Each opportunistic resource (entity) is associated with a particular agent Node, and their validity
     *     window (start to end timestamps) never overlap.</li>
     *     <li>The current active opportunistic resource (window) is always the latest to expire.</li>
     *     <li>There are no resources that will only be available in the future, i.e.: <tt>start <= now.</tt></li>
     *     <li>Tasks can only be assigned to opportunistic CPUs if their availability window hasn't yet expired.</li>
     *     <li>Opportunistic resource entities express their availability as timestamps. We ignore clock skew issues
     *     here and will handle them elsewhere. We also assume a small risk of a few tasks being allocated to expired
     *     windows, or windows expiring too soon when timestamps from these resources are skewed in relation to the
     *     master (active leader) clock.</li>
     * </ul>
     *
     * @return the active opportunistic resources availability for each agent node
     */
    @Override
    public Map<String, OpportunisticCpuAvailability> getOpportunisticCpus() {
        return kubeApiFacade.getOpportunisticResourceInformer().getIndexer().list().stream().collect(Collectors.toMap(
                V1OpportunisticResource::getInstanceId,
                resource -> new OpportunisticCpuAvailability(resource.getUid(), resource.getEnd(), resource.getCpus()),
                BinaryOperator.maxBy(EXPIRES_AT_COMPARATOR)
        ));
    }

    private boolean isNotExpired(OpportunisticCpuAvailability availability) {
        return !titusRuntime.getClock().isPast(availability.getExpiresAt().toEpochMilli());
    }
}
