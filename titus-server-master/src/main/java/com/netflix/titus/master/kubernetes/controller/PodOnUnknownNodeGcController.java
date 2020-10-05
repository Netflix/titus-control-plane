/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.controller;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.master.mesos.kubeapiserver.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Singleton
public class PodOnUnknownNodeGcController extends BaseGcController<V1Pod> {
    public static final String POD_ON_UNKNOWN_NODE_GC_CONTROLLER = "podOnUnknownNodeGcController";
    public static final String POD_ON_UNKNOWN_NODE_GC_CONTROLLER_DESCRIPTION = "GC pods on unknown nodes.";

    private static final Logger logger = LoggerFactory.getLogger(PodOnUnknownNodeGcController.class);
    private final KubeApiFacade kubeApiFacade;

    @Inject
    public PodOnUnknownNodeGcController(
            TitusRuntime titusRuntime,
            LocalScheduler scheduler,
            @Named(POD_ON_UNKNOWN_NODE_GC_CONTROLLER) FixedIntervalTokenBucketConfiguration tokenBucketConfiguration,
            @Named(POD_ON_UNKNOWN_NODE_GC_CONTROLLER) ControllerConfiguration controllerConfiguration,
            KubeApiFacade kubeApiFacade
    ) {
        super(
                POD_ON_UNKNOWN_NODE_GC_CONTROLLER,
                POD_ON_UNKNOWN_NODE_GC_CONTROLLER_DESCRIPTION,
                titusRuntime,
                scheduler,
                tokenBucketConfiguration,
                controllerConfiguration
        );
        this.kubeApiFacade = kubeApiFacade;
    }

    @Override
    public boolean shouldGc() {
        return kubeApiFacade.getNodeInformer().hasSynced() && kubeApiFacade.getPodInformer().hasSynced();
    }

    @Override
    public List<V1Pod> getItemsToGc() {
        Set<String> knownNodeNames = kubeApiFacade.getNodeInformer().getIndexer().list()
                .stream()
                .map(n -> KubeUtil.getMetadataName(n.getMetadata()))
                .collect(Collectors.toSet());
        return kubeApiFacade.getPodInformer().getIndexer().list()
                .stream()
                .filter(p -> isPodOnUnknownNode(p, knownNodeNames))
                .collect(Collectors.toList());
    }

    @Override
    public boolean gcItem(V1Pod item) {
        return GcControllerUtil.deletePod(kubeApiFacade, logger, item);
    }

    @VisibleForTesting
    boolean isPodOnUnknownNode(V1Pod pod, Set<String> knownNodeNames) {
        if (pod == null || pod.getSpec() == null) {
            return false;
        }

        String nodeName = pod.getSpec().getNodeName();
        return StringExt.isNotEmpty(nodeName) && !knownNodeNames.contains(nodeName);
    }
}