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

import javax.inject.Named;
import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;

import static com.netflix.titus.master.kubernetes.controller.NodeGcController.NODE_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodDeletionGcController.POD_DELETION_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodOnUnknownNodeGcController.POD_ON_UNKNOWN_NODE_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodTerminalGcController.POD_TERMINAL_GC_CONTROLLER;
import static com.netflix.titus.master.kubernetes.controller.PodUnknownGcController.POD_UNKNOWN_GC_CONTROLLER;

public class KubeControllerModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(NodeGcController.class).asEagerSingleton();
        bind(PodOnUnknownNodeGcController.class).asEagerSingleton();
        bind(PodDeletionGcController.class).asEagerSingleton();
        bind(PodTerminalGcController.class).asEagerSingleton();
        bind(PodUnknownGcController.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public KubeControllerConfiguration getKubeControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(KubeControllerConfiguration.class);
    }

    @Provides
    @Singleton
    @Named(NODE_GC_CONTROLLER)
    public FixedIntervalTokenBucketConfiguration getNodeGcControllerTokenBucketConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(FixedIntervalTokenBucketConfiguration.class, "titus.kubernetes.controller." + NODE_GC_CONTROLLER);
    }

    @Provides
    @Singleton
    @Named(NODE_GC_CONTROLLER)
    public ControllerConfiguration getNodeGcControllerConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(ControllerConfiguration.class, "titus.kubernetes.controller." + NODE_GC_CONTROLLER);
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
}
