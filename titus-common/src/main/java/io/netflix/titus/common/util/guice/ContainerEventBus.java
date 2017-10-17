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

package io.netflix.titus.common.util.guice;

import java.lang.reflect.Method;

/**
 * Existing Governator implementation of the event bus dispatcher, but also Guava's evenbus do not
 * preserve the execution order of the event publishing. It is critical for an instance lifecycle management,
 * that it executes according to dependency graph or in the reverse order.
 */
public interface ContainerEventBus {

    interface ContainerEvent {
    }

    class ContainerStartedEvent implements ContainerEvent {
    }

    interface ContainerEventListener<T extends ContainerEvent> {
        void onEvent(T event);
    }

    void registerListener(Object instance, Method method, Class<? extends ContainerEvent> eventType);

    <T extends ContainerEvent> void registerListener(Class<T> eventType, ContainerEventListener<T> eventListener);

    void registerListener(ContainerEventListener<? extends ContainerEvent> eventListener);

    void submitInOrder(ContainerEvent event);

    void submitInReversedOrder(ContainerEvent event);
}
