/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler.internal.provider;

import java.util.List;

import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProvider;
import com.netflix.titus.common.runtime.TitusRuntime;

public class ActionProviderSelectorFactory<DATA> {

    private final String name;
    private final List<ReconcilerActionProvider<DATA>> actionProviders;
    private final TitusRuntime titusRuntime;

    public ActionProviderSelectorFactory(String name,
                                         List<ReconcilerActionProvider<DATA>> actionProviders,
                                         TitusRuntime titusRuntime) {
        this.name = name;
        this.actionProviders = actionProviders;
        this.titusRuntime = titusRuntime;
    }

    public ActionProviderSelector<DATA> create() {
        return new ActionProviderSelector<>(name, actionProviders, titusRuntime);
    }
}
