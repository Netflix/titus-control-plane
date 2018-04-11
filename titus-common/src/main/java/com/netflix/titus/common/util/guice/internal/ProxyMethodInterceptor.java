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
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ReflectionExt;
import com.netflix.titus.common.util.guice.ActivationLifecycle;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.proxy.LoggingProxyBuilder;
import com.netflix.titus.common.util.proxy.ProxyInvocationChain;
import com.netflix.titus.common.util.proxy.ProxyInvocationHandler;
import com.netflix.titus.common.util.proxy.internal.DefaultProxyInvocationChain;
import com.netflix.titus.common.util.proxy.internal.GuardingInvocationHandler;
import com.netflix.titus.common.util.proxy.internal.SpectatorInvocationHandler;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@Singleton
public class ProxyMethodInterceptor implements MethodInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ProxyMethodInterceptor.class);

    private final LoadingCache<Class<?>, InstanceWrapper> instanceToWrapperMap = CacheBuilder.newBuilder()
            .build(new CacheLoader<Class<?>, InstanceWrapper>() {
                @Override
                public InstanceWrapper load(Class<?> instanceType) throws Exception {
                    return buildProxy(instanceType);
                }
            });

    private final LoadingCache<ClassMethodPair, Optional<InstanceWrapper>> methodToWrapperMap = CacheBuilder.newBuilder()
            .build(new CacheLoader<ClassMethodPair, Optional<InstanceWrapper>>() {
                @Override
                public Optional<InstanceWrapper> load(ClassMethodPair id) throws Exception {
                    InstanceWrapper wrapper = instanceToWrapperMap.get(id.instanceType);
                    if (wrapper.isWrapped(id.method)) {
                        return Optional.of(wrapper);
                    }
                    return Optional.empty();
                }
            });

    private final Provider<ActivationLifecycle> activationLifecycle;
    private final Provider<TitusRuntime> titusRuntimeProvider;

    @Inject
    public ProxyMethodInterceptor(Provider<ActivationLifecycle> activationLifecycle, Provider<TitusRuntime> titusRuntimeProvider) {
        this.activationLifecycle = activationLifecycle;
        this.titusRuntimeProvider = titusRuntimeProvider;
    }

    @Override
    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
        Class<?> instanceType = ((Method) methodInvocation.getStaticPart()).getDeclaringClass();
        Method effectiveMethod = ReflectionExt.findInterfaceMethod(methodInvocation.getMethod()).orElse(methodInvocation.getMethod());
        ClassMethodPair id = new ClassMethodPair(instanceType, effectiveMethod);
        Optional<InstanceWrapper> wrapper = methodToWrapperMap.get(id);
        if (wrapper.isPresent()) {
            return wrapper.get().invoke(methodInvocation, effectiveMethod);
        }
        return methodInvocation.proceed();
    }

    private InstanceWrapper buildProxy(Class<?> instanceType) {
        Optional<ProxyConfiguration> configurationOpt = findProxyConfiguration(instanceType);
        if (!configurationOpt.isPresent()) {
            return new InstanceWrapper();
        }
        Optional<Class<?>> interfOpt = findInterface(instanceType);
        if (!interfOpt.isPresent()) {
            return new InstanceWrapper();
        }
        return new InstanceWrapper(configurationOpt.get(), interfOpt.get());

    }

    private Optional<ProxyConfiguration> findProxyConfiguration(Class<?> instanceType) {
        ProxyConfiguration configuration = instanceType.getAnnotation(ProxyConfiguration.class);
        if (configuration == null || CollectionsExt.isNullOrEmpty(configuration.types())) {
            return Optional.empty();
        }
        return Optional.of(configuration);
    }

    private Optional<Class<?>> findInterface(Class<?> instanceType) {
        Class<?>[] interfaces = instanceType.getInterfaces();
        if (CollectionsExt.isNullOrEmpty(interfaces)) {
            return Optional.empty();
        }
        Class interf = interfaces[0];
        if (interfaces.length > 1) {
            logger.warn("Multiple interfaces found, while proxy supports single interfaces only: {}. Wrapping {}",
                    instanceType.getName(),
                    interf.getName()
            );
        }
        return Optional.of(interf);
    }

    static class ClassMethodPair {
        private final Class<?> instanceType;
        private final Method method;

        ClassMethodPair(Class<?> instanceType, Method method) {
            this.instanceType = instanceType;
            this.method = method;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ClassMethodPair that = (ClassMethodPair) o;

            if (instanceType != that.instanceType) {
                return false;
            }
            return method != null ? method.equals(that.method) : that.method == null;
        }

        @Override
        public int hashCode() {
            int result = instanceType != null ? instanceType.hashCode() : 0;
            result = 31 * result + (method != null ? method.hashCode() : 0);
            return result;
        }
    }

    private final static ProxyInvocationHandler<MethodInvocation> EXECUTING_HANDLER =
            (proxy, method, args, nativeHandler, chain) -> nativeHandler.proceed();

    class InstanceWrapper {

        private final Class<?> interf;
        private final ProxyInvocationChain<MethodInvocation> chain;

        InstanceWrapper() {
            this.interf = null;
            this.chain = new DefaultProxyInvocationChain<>(Collections.singletonList(EXECUTING_HANDLER));
        }

        InstanceWrapper(ProxyConfiguration configuration, Class<?> interf) {
            this.interf = interf;
            this.chain = new DefaultProxyInvocationChain<>(buildInterceptors(configuration, interf));
        }

        boolean isWrapped(Method method) {
            try {
                return interf != null && interf.getMethod(method.getName(), method.getParameterTypes()) != null;
            } catch (NoSuchMethodException e) {
                return false;
            }
        }

        Object invoke(MethodInvocation methodInvocation, Method effectiveMethod) throws Throwable {
            return chain.invoke(methodInvocation.getThis(), effectiveMethod, methodInvocation.getArguments(), methodInvocation);
        }

        private List<ProxyInvocationHandler<MethodInvocation>> buildInterceptors(ProxyConfiguration configuration, Class<?> interf) {
            List<ProxyInvocationHandler<MethodInvocation>> interceptors = new ArrayList<>();
            for (ProxyType type : configuration.types()) {
                switch (type) {
                    case Logging:
                        interceptors.add(new LoggingProxyBuilder(interf, null).titusRuntime(titusRuntimeProvider.get()).buildHandler());
                        break;
                    case Spectator:
                        interceptors.add(new SpectatorInvocationHandler<>(interf, titusRuntimeProvider.get(), true));
                        break;
                    case ActiveGuard:
                        interceptors.add(new GuardingInvocationHandler<>(interf, instance -> activationLifecycle.get().isActive(instance)));
                        break;
                }
            }
            interceptors.add(EXECUTING_HANDLER);
            return interceptors;
        }
    }
}
