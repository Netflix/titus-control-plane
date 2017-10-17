/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util.guice.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.inject.Singleton;

import com.google.common.reflect.TypeToken;
import io.netflix.titus.common.util.guice.ContainerEventBus;

@Singleton
public class DefaultContainerEventBus implements ContainerEventBus {

    private final Method eventListenerMethod;

    private final List<SubscriberProxy> subscriptionsInRegistrationOrder = new CopyOnWriteArrayList<>();
    private final List<SubscriberProxy> subscriptionsInReverseOrder = new CopyOnWriteArrayList<>();

    public DefaultContainerEventBus() {
        try {
            this.eventListenerMethod = ContainerEventListener.class.getDeclaredMethod("onEvent", ContainerEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to cache ApplicationEventListener method", e);
        }
    }

    @Override
    public void registerListener(Object instance, Method method, Class<? extends ContainerEvent> eventType) {
        addSubscription(new SubscriberProxy(instance, method, eventType));
    }

    @Override
    public <T extends ContainerEvent> void registerListener(Class<T> eventType, ContainerEventListener<T> eventListener) {
        addSubscription(new SubscriberProxy(eventListener, eventListenerMethod, eventType));
    }

    @Override
    public void registerListener(ContainerEventListener<? extends ContainerEvent> eventListener) {
        Type[] genericInterfaces = eventListener.getClass().getGenericInterfaces();
        for (Type type : genericInterfaces) {
            if (ContainerEventListener.class.isAssignableFrom(TypeToken.of(type).getRawType())) {
                ParameterizedType ptype = (ParameterizedType) type;
                Class<?> rawType = TypeToken.of(ptype.getActualTypeArguments()[0]).getRawType();
                addSubscription(new SubscriberProxy(eventListener, eventListenerMethod, rawType));
                return;
            }
        }
    }

    @Override
    public void submitInOrder(ContainerEvent event) {
        submit(event, subscriptionsInRegistrationOrder);
    }

    @Override
    public void submitInReversedOrder(ContainerEvent event) {
        submit(event, subscriptionsInReverseOrder);
    }

    private void submit(ContainerEvent event, List<SubscriberProxy> subscriptions) {
        Iterator<SubscriberProxy> it = subscriptions.iterator();
        while (it.hasNext()) {
            SubscriberProxy proxy = it.next();
            try {
                proxy.invokeEventHandler(event);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void addSubscription(SubscriberProxy proxy) {
        subscriptionsInRegistrationOrder.add(proxy);
        subscriptionsInReverseOrder.add(proxy);
    }

    static class SubscriberProxy {

        private final Object handlerInstance;
        private final Method handlerMethod;
        private final Class<?> acceptedType;

        SubscriberProxy(Object handlerInstance, Method handlerMethod, Class<?> acceptedType) {
            this.handlerInstance = handlerInstance;
            this.handlerMethod = handlerMethod;
            this.acceptedType = acceptedType;
        }

        void invokeEventHandler(ContainerEvent event)
                throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            if (acceptedType.isAssignableFrom(event.getClass())) {
                if (!handlerMethod.isAccessible()) {
                    handlerMethod.setAccessible(true);
                }
                handlerMethod.invoke(handlerInstance, event);
            }
        }
    }
}
