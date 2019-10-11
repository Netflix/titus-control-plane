/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.common.util.guice.internal;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import javax.inject.Singleton;

import com.google.inject.spi.ProvisionListener;
import com.netflix.governator.annotations.SuppressLifecycleUninitialized;
import com.netflix.titus.common.util.ReflectionExt;
import com.netflix.titus.common.util.guice.ActivationLifecycle;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice {@link ProvisionListener} that scans services for presence of {@link Activator} and {@link Deactivator}
 * annotations, and adds them to activation/deactivation lifecycle.
 * <p>
 * Deactivations happen in the reverse order of activations.
 */
@Singleton
@SuppressLifecycleUninitialized
public class ActivationProvisionListener implements ActivationLifecycle, ProvisionListener {

    private static final Logger logger = LoggerFactory.getLogger(ActivationProvisionListener.class);

    /**
     * Keep all services that need activation/deactivation as a {@link Set} to avoid activating the same instance
     * multiple times. <b>Note:</b> the {@link Set} implementation used <b>must</b> preserve insertion order, since
     * deactivation needs to happen in the reverse order of activation.
     */
    private final Set<ServiceHolder> services = new CopyOnWriteArraySet<>();

    private long activationTime = -1;

    @Override
    public <T> void onProvision(ProvisionInvocation<T> provision) {
        T injectee = provision.provision();
        if (injectee == null) {
            return;
        }
        ServiceHolder holder = new ServiceHolder(injectee);
        if (holder.isEmpty()) {
            return;
        }
        services.add(holder);
    }

    @Override
    public <T> boolean isActive(T instance) {
        for (ServiceHolder service : services) {
            if (service.getInjectee() == instance) {
                return service.isActivated();
            }
        }
        return false;
    }

    @Override
    public void activate() {
        long startTime = System.currentTimeMillis();
        logger.info("Activating services");

        services.forEach(ServiceHolder::activate);

        this.activationTime = System.currentTimeMillis() - startTime;
        logger.info("Service activation finished in {}[ms]", activationTime);
    }

    @Override
    public void deactivate() {
        long startTime = System.currentTimeMillis();
        logger.info("Deactivating services");

        List<ServiceHolder> reversed = new ArrayList<>(services);
        Collections.reverse(reversed);

        reversed.forEach(ServiceHolder::deactivate);
        logger.info("Service deactivation finished in {}[ms]", System.currentTimeMillis() - startTime);
    }

    @Override
    public long getActivationTimeMs() {
        return activationTime;
    }

    @Override
    public List<Pair<String, Long>> getServiceActionTimesMs() {
        return services.stream()
                .map(holder -> Pair.of(holder.getName(), holder.getActivationTime()))
                .collect(Collectors.toList());
    }

    static class ServiceHolder {
        private final Object injectee;
        private final String name;

        private final List<Method> activateMethods;
        private final List<Method> deactivateMethods;

        private boolean activated;
        private long activationTime = -1;

        ServiceHolder(Object injectee) {
            this.injectee = injectee;
            this.name = injectee.getClass().getSimpleName();
            this.activateMethods = ReflectionExt.findAnnotatedMethods(injectee, Activator.class);
            this.deactivateMethods = ReflectionExt.findAnnotatedMethods(injectee, Deactivator.class);
        }

        Object getInjectee() {
            return injectee;
        }

        String getName() {
            return name;
        }

        boolean isActivated() {
            return activated;
        }

        long getActivationTime() {
            return activationTime;
        }

        boolean isEmpty() {
            return activateMethods.isEmpty() && deactivateMethods.isEmpty();
        }

        void activate() {
            logger.info("Activating service {}", name);

            if (activated) {
                logger.warn("Skipping activation process for service {}, as it is already active", name);
                return;
            }

            long startTime = System.currentTimeMillis();
            for (Method m : activateMethods) {
                try {
                    m.invoke(injectee);
                } catch (Exception e) {
                    logger.warn("Service {} activation failure after {}[ms]", name, System.currentTimeMillis() - startTime, e);
                    throw new IllegalStateException(name + " service activation failure", e);
                }
            }
            activated = true;
            this.activationTime = System.currentTimeMillis() - startTime;
            logger.warn("Service {} activated in {}[ms]", name, activationTime);
        }

        void deactivate() {
            logger.info("Deactivating service {}", name);

            if (!activated) {
                logger.warn("Skipping deactivation process for service {}, as it is not active", name);
                return;
            }
            long startTime = System.currentTimeMillis();

            for (Method m : deactivateMethods) {
                try {
                    m.invoke(injectee);
                } catch (Exception e) {
                    // Do not propagate exception in the deactivation phase
                    logger.warn("Service {} deactivation failure after {}[ms]", name, System.currentTimeMillis() - startTime, e);
                }
            }

            long deactivationTime = System.currentTimeMillis() - startTime;
            logger.warn("Service {} deactivated in {}[ms]", name, deactivationTime);

            activated = false;
            activationTime = -1;
        }

        /**
         * Two service holders are considered equal if they are holding the same instance (based on object identity).
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ServiceHolder)) {
                return false;
            }
            ServiceHolder other = (ServiceHolder) obj;
            return this.injectee == other.injectee;
        }

        @Override
        public int hashCode() {
            return injectee.hashCode();
        }
    }
}
