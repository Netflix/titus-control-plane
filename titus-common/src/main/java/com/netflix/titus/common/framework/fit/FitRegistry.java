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
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A container for FIT action kinds.
 */
public interface FitRegistry {

    /**
     * Returns list of all known action descriptors.
     */
    List<FitActionDescriptor> getFitActionDescriptors();

    /**
     * Returns a FIT action factory for the provided configuration data.
     * <h1>Why FIT action is not created immediately?</h1>
     * The factory method is returned, as to finish the action construction process the {@link FitInjection} instance
     * is needed, and it is not always available at this stage.
     */
    Function<FitInjection, FitAction> newFitActionFactory(String actionKind, String id, Map<String, String> properties);

    /**
     * Add new FIT action kind to the registry.
     *
     * @param descriptor action descriptor
     * @param factory    action creator
     */
    void registerActionKind(FitActionDescriptor descriptor, BiFunction<String, Map<String, String>, Function<FitInjection, FitAction>> factory);
}
