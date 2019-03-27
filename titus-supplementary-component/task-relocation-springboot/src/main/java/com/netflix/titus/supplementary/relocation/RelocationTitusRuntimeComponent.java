/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.framework.fit.FitComponent;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.adapter.GrpcFitInterceptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RelocationTitusRuntimeComponent {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal(true);

    @Bean
    public TitusRuntime getTitusRuntime() {
        FitFramework fitFramework = titusRuntime.getFitFramework();
        if (fitFramework.isActive()) {
            FitComponent root = fitFramework.getRootComponent();
            root.createChild(GrpcFitInterceptor.COMPONENT);
        }

        return titusRuntime;
    }

    @Bean
    public Registry getRegistry(TitusRuntime titusRuntime) {
        return titusRuntime.getRegistry();
    }
}
