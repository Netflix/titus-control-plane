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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import javax.inject.Singleton;

import com.google.inject.spi.ProvisionListener;
import com.netflix.governator.annotations.SuppressLifecycleUninitialized;
import com.netflix.titus.common.util.GraphExt;
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
 */
@Singleton
@SuppressLifecycleUninitialized
public class ActivationProvisionListener implements ActivationLifecycle, ProvisionListener {

    private static final Logger logger = LoggerFactory.getLogger(ActivationProvisionListener.class);

    private final List<ServiceHolder> services = new CopyOnWriteArrayList<>();

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
        enforceOrderInvariants();

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

    /**
     * Objects are by default activated in the order of their provisioning. However it is possible to specify direct
     * dependency between objects, which may require reordering.
     */
    private void enforceOrderInvariants() {
        List<ServiceHolder> ordered = GraphExt.order(new ArrayList<>(services), this::shouldRunBefore);
        this.services.clear();
        this.services.addAll(ordered);
    }

    private Boolean shouldRunBefore(ServiceHolder before, ServiceHolder after) {
        Class<?> beforeClass = before.getInjectee().getClass();
        for (Class<?> beforeInterf : beforeClass.getInterfaces()) {
            if (after.getServicesToRunFirst().contains(beforeInterf)) {
                return true;
            }
        }
        return after.getServicesToRunFirst().contains(beforeClass);
    }

    static class ServiceHolder {
        private final Object injectee;
        private final String name;

        private final List<Method> activateMethods;
        private final List<Method> deactivateMethods;
        private final Set<Class<?>> servicesToRunFirst;

        private boolean activated;
        private long activationTime = -1;

        ServiceHolder(Object injectee) {
            this.injectee = injectee;
            this.name = injectee.getClass().getSimpleName();
            this.activateMethods = ReflectionExt.findAnnotatedMethods(injectee, Activator.class);
            this.deactivateMethods = ReflectionExt.findAnnotatedMethods(injectee, Deactivator.class);

            this.servicesToRunFirst = resolveServicesToRunFirst();
        }

        private Set<Class<?>> resolveServicesToRunFirst() {
            for (Method method : activateMethods) {
                Class<?>[] after = method.getAnnotation(Activator.class).after();
                if (after.length > 0) {
                    return new HashSet<>(Arrays.asList(after));
                }
            }
            return Collections.emptySet();
        }

        Object getInjectee() {
            return injectee;
        }

        String getName() {
            return name;
        }

        Set<Class<?>> getServicesToRunFirst() {
            return servicesToRunFirst;
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
                    logger.warn("Service {} activation failure after {}[ms]", name, System.currentTimeMillis() - startTime);
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
                    logger.warn("Service {} deactivation failure after {}[ms]", name, System.currentTimeMillis() - startTime);
                    logger.info("Deactivation error", e);
                }
            }

            long deactivationTime = System.currentTimeMillis() - startTime;
            logger.warn("Service {} deactivated in {}[ms]", name, deactivationTime);

            activated = false;
            activationTime = -1;
        }
    }
}
