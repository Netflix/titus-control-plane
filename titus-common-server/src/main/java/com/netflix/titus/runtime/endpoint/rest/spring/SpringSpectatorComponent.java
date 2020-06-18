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

package com.netflix.titus.runtime.endpoint.rest.spring;

import com.netflix.titus.common.runtime.TitusRuntime;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class SpringSpectatorComponent {

    @Bean
    public SpringSpectatorWebConfigurer getSpringSpectatorWebConfigurer(TitusRuntime titusRuntime) {
        return new SpringSpectatorWebConfigurer(titusRuntime);
    }

    static class SpringSpectatorWebConfigurer implements WebMvcConfigurer {

        private final TitusRuntime titusRuntime;

        public SpringSpectatorWebConfigurer(TitusRuntime titusRuntime) {
            this.titusRuntime = titusRuntime;
        }

        @Override
        public void addInterceptors(InterceptorRegistry registry) {
            registry.addInterceptor(new SpringSpectatorInterceptor(titusRuntime));
        }
    }
}
