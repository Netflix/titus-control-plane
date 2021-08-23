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

package com.netflix.titus.federation.startup;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.jhiccup.JHiccupComponent;
import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.environment.MyEnvironments;
import com.netflix.titus.common.runtime.SystemAbortListener;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.runtime.internal.LoggingSystemAbortListener;
import com.netflix.titus.common.runtime.internal.LoggingSystemLogService;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CompositeCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.SpectatorCodeInvariants;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorClientFactoryComponent;
import com.netflix.titus.runtime.connector.common.reactor.GrpcToReactorServerFactoryComponent;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolveComponent;
import com.netflix.titus.runtime.endpoint.rest.RestAddOnsComponent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

@Configuration
@Import({
        JHiccupComponent.class,
        CallMetadataResolveComponent.class,
        GrpcToReactorClientFactoryComponent.class,
        GrpcToReactorServerFactoryComponent.class,
        RestAddOnsComponent.class,
})
public class TitusFederationRuntimeComponent {

    @Bean
    public Registry getRegistry() {
        return new DefaultRegistry();
    }

    @Bean
    public SystemLogService getSystemLogService() {
        return LoggingSystemLogService.getInstance();
    }

    @Bean
    public SystemAbortListener getSystemAbortListener() {
        return LoggingSystemAbortListener.getDefault();
    }

    @Bean
    public TitusRuntime getTitusRuntime(Environment environment, SystemLogService systemLogService, SystemAbortListener systemAbortListener, Registry registry) {
        CodeInvariants codeInvariants = new CompositeCodeInvariants(
                LoggingCodeInvariants.getDefault(),
                new SpectatorCodeInvariants(registry.createId("titus.runtime.invariant.violations"), registry)
        );
        return new DefaultTitusRuntime(MyEnvironments.newSpring(environment), codeInvariants, systemLogService, true, systemAbortListener, registry);
    }

    @Bean
    public TitusFederationConfiguration getTitusFederationConfiguration(MyEnvironment environment) {
        return Archaius2Ext.newConfiguration(TitusFederationConfiguration.class, environment);
    }

    @Bean
    public GrpcConfiguration getGrpcConfiguration(MyEnvironment environment) {
        return Archaius2Ext.newConfiguration(GrpcConfiguration.class, environment);
    }
}
