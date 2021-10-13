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

import java.time.Duration;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.internal.DefaultLocalScheduler;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.master.kubernetes.controller.BaseGcController.GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.DefaultKubeJobManagementReconciler.GC_UNKNOWN_PODS;
import static com.netflix.titus.master.kubernetes.controller.PersistentVolumeClaimGcController.PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PersistentVolumeReclaimController.PERSISTENT_VOLUME_RECLAIM_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PersistentVolumeUnassociatedGcController.PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodDeletionGcController.POD_DELETION_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodOnUnknownNodeGcController.POD_ON_UNKNOWN_NODE_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodTerminalGcController.POD_TERMINAL_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodUnknownGcController.POD_UNKNOWN_GC_CONTROLLER;

public class KubeControllerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(KubeJobManagementReconciler.class).to(DefaultKubeJobManagementReconciler.class);
        bind(PodOnUnknownNodeGcController.class).asEagerSingleton();
        bind(PodDeletionGcController.class).asEagerSingleton();
        bind(PodTerminalGcController.class).asEagerSingleton();
        bind(PodUnknownGcController.class).asEagerSingleton();
        bind(PersistentVolumeUnassociatedGcController.class).asEagerSingleton();
        bind(PersistentVolumeClaimGcController.class).asEagerSingleton();
        bind(PersistentVolumeReclaimController.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    @Named(GC_CONTROLLER)
    public LocalScheduler getLocalScheduler(TitusRuntime titusRuntime) {
        return new DefaultLocalScheduler(Duration.ofMillis(100), Schedulers.elastic(), titusRuntime.getClock(), titusRuntime.getRegistry());
    }

    @Provides
    @Singleton
    public KubeControllerConfiguration getKubeControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(KubeControllerConfiguration.class);
    }

    @Provides
    @Singleton
    @Named(GC_UNKNOWN_PODS)
    public FixedIntervalTokenBucketConfiguration getGcUnknownPodsTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titusMaster.kube.gcUnknownPodsTokenBucket");
    }

    @Provides
    @Singleton
    @Named(POD_ON_UNKNOWN_NODE_GC_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getPodOnUnknownNodeGcControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + POD_ON_UNKNOWN_NODE_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(POD_ON_UNKNOWN_NODE_GC_CONTROLLER)
    public ControllerConfiguration getPodOnUnknownNodeGcControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + POD_ON_UNKNOWN_NODE_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(POD_DELETION_GC_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getPodDeletionGcControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + POD_DELETION_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(POD_DELETION_GC_CONTROLLER)
    public ControllerConfiguration getPodDeletionGcControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + POD_DELETION_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(POD_TERMINAL_GC_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getPodTerminalGcControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + POD_TERMINAL_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(POD_TERMINAL_GC_CONTROLLER)
    public ControllerConfiguration getPodTerminalGcControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + POD_TERMINAL_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(POD_UNKNOWN_GC_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getPodUnknownGcControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + POD_UNKNOWN_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(POD_UNKNOWN_GC_CONTROLLER)
    public ControllerConfiguration getPodUnknownGcControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + POD_UNKNOWN_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getPersistentVolumeUnassociatedGcControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER)
    public ControllerConfiguration getPersistentVolumeUnassociatedGcControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + PERSISTENT_VOLUME_UNASSOCIATED_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getPersistentVolumeClaimGcControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER)
    public ControllerConfiguration getPersistentVolumeClaimGcControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + PERSISTENT_VOLUME_CLAIM_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(PERSISTENT_VOLUME_RECLAIM_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getPersistentVolumeReclaimControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + PERSISTENT_VOLUME_RECLAIM_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(PERSISTENT_VOLUME_RECLAIM_CONTROLLER)
    public ControllerConfiguration getPersistentVolumeReclaimControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + PERSISTENT_VOLUME_RECLAIM_CONTROLLER);
    }
}
