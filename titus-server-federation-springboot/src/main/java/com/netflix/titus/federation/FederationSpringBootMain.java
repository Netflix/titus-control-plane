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

package com.netflix.titus.federation;

import com.netflix.titus.api.health.HealthIndicator;
import com.netflix.titus.api.health.HealthIndicators;
import com.netflix.titus.federation.startup.TitusFederationComponent;
import com.netflix.titus.federation.startup.TitusFederationRuntimeComponent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({
        TitusFederationRuntimeComponent.class,
        TitusFederationComponent.class
})
public class FederationSpringBootMain {

    @Bean
    public HealthIndicator getHealthIndicator() {
        return HealthIndicators.alwaysHealthy();
    }

    public static void main(String[] args) {
        SpringApplication.run(FederationSpringBootMain.class, args);
    }
}
