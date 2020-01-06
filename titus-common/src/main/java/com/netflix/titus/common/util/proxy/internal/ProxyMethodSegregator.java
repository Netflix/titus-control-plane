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

package com.netflix.titus.common.util.proxy.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.netflix.titus.common.util.proxy.annotation.ObservableResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

class ProxyMethodSegregator<API> {

    private final Set<Method> followedObservables = new HashSet<>();
    private final Set<Method> followedFlux = new HashSet<>();
    private final Set<Method> followedCompletables = new HashSet<>();
    private final Set<Method> followedMonos = new HashSet<>();

    ProxyMethodSegregator(Class<API> apiInterface, boolean followObservableResults, Set<Method> includedMethodSet) {
        boolean enabledByDefault = followObservableResults || enablesTarget(apiInterface.getAnnotations());
        for (Method method : includedMethodSet) {
            boolean isObservableResult = method.getReturnType().isAssignableFrom(Observable.class);
            boolean isFluxResult = method.getReturnType().isAssignableFrom(Flux.class);
            boolean isCompletableResult = method.getReturnType().isAssignableFrom(Completable.class);
            boolean isMonoResult = method.getReturnType().isAssignableFrom(Mono.class);
            if (isObservableResult || isFluxResult || isCompletableResult || isMonoResult) {
                boolean methodEnabled = enabledByDefault || enablesTarget(method.getAnnotations());
                if (methodEnabled) {
                    if (isObservableResult) {
                        followedObservables.add(method);
                    } else if (isFluxResult) {
                        followedFlux.add(method);
                    } else if (isCompletableResult) {
                        followedCompletables.add(method);
                    } else if (isMonoResult) {
                        followedMonos.add(method);
                    }
                }
            }
        }
    }

    Set<Method> getFollowedObservables() {
        return followedObservables;
    }

    Set<Method> getFollowedFlux() {
        return followedFlux;
    }

    Set<Method> getFollowedCompletables() {
        return followedCompletables;
    }

    Set<Method> getFollowedMonos() {
        return followedMonos;
    }

    private boolean enablesTarget(Annotation[] annotations) {
        Optional<Annotation> result = find(annotations, ObservableResult.class);
        return result.isPresent() && ((ObservableResult) result.get()).enabled();
    }

    private Optional<Annotation> find(Annotation[] annotations, Class<? extends Annotation> expected) {
        for (Annotation current : annotations) {
            if (current.annotationType().equals(expected)) {
                return Optional.of(current);
            }
        }
        return Optional.empty();
    }
}
