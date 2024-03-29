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

package com.netflix.titus.common.jhiccup;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JHiccupComponent {

    @Bean
    public HiccupMeter getHiccupMeter(HiccupRecorderConfiguration configuration, Registry registry) {
        return new HiccupMeter(configuration, registry);
    }

    @Bean
    public HiccupRecorderConfiguration getHiccupRecorderConfiguration(MyEnvironment environment) {
        return Archaius2Ext.newConfiguration(HiccupRecorderConfiguration.class, environment);
    }
}
