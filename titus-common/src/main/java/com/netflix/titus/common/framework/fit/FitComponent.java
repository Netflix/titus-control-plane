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

package com.netflix.titus.common.framework.fit;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * {@link FitComponent} represents a part of an application, and it is a container for the FIT injection points.
 * It may contain sub-components to an arbitrary depth.
 */
public interface FitComponent {

    /**
     * Globally unique component identifier.
     */
    String getId();

    /**
     * Adds new sub-component instance.
     */
    FitComponent createChild(String childId);

    /**
     * Gets all direct children of this component.
     */
    List<FitComponent> getChildren();

    /**
     * Returns a child component with the given id.
     *
     * @throws IllegalArgumentException if a child with the given id does not exist
     */
    FitComponent getChild(String id);

    /**
     * Returns a child component with the given id if found or {@link Optional#empty()} otherwise.
     */
    Optional<FitComponent> findChild(String id);

    /**
     * Adds new FIT injection.
     */
    FitComponent addInjection(FitInjection fitInjection);

    /**
     * Returns all FIT injections associated with this component.
     */
    List<FitInjection> getInjections();

    /**
     * Returns the owned {@link FitInjection} instance with the given id.
     *
     * @throws IllegalArgumentException if an instance with the given id does not exist
     */
    FitInjection getInjection(String id);

    /**
     * Returns the owned {@link FitInjection} instance with the given id if found or {@link Optional#empty()} otherwise.
     */
    Optional<FitInjection> findInjection(String name);

    /**
     * Visitor pattern's acceptor that traverses the component/injector hierarchy in breadth-first order.
     */
    void acceptInjections(Consumer<FitInjection> evaluator);
}
