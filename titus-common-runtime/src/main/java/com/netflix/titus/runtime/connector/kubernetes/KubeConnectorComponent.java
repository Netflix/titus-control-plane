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

package com.netflix.titus.runtime.connector.kubernetes;

import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import io.kubernetes.client.openapi.ApiClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class KubeConnectorComponent {

    @Bean
    public KubeConnectorConfiguration getKubeConnectorConfiguration(MyEnvironment environment) {
        return Archaius2Ext.newConfiguration(KubeConnectorConfiguration.class, "titus.kubeClient", environment);
    }

    @Bean
    public KubeApiFacade getKubeApiFacade(KubeConnectorConfiguration configuration, ApiClient apiClient, TitusRuntime titusRuntime) {
        return new DefaultKubeApiFacade(configuration, apiClient, titusRuntime);
    }
}
