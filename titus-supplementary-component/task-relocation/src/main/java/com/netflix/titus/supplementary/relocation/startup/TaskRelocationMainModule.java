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

package com.netflix.titus.supplementary.relocation.startup;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.validator.PassJobValidator;
import com.netflix.titus.common.model.validator.EntityValidator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import com.netflix.titus.common.runtime.internal.LoggingSystemAbortListener;
import com.netflix.titus.common.runtime.internal.LoggingSystemLogService;
import com.netflix.titus.common.util.archaius2.Archaius2ConfigurationLogger;
import com.netflix.titus.common.util.code.CodeInvariants;
import com.netflix.titus.common.util.code.CompositeCodeInvariants;
import com.netflix.titus.common.util.code.LoggingCodeInvariants;
import com.netflix.titus.common.util.code.SpectatorCodeInvariants;
import com.netflix.titus.runtime.TitusEntitySanitizerModule;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.descheduler.DeschedulerModule;
import com.netflix.titus.supplementary.relocation.endpoint.TaskRelocationEndpointModule;
import com.netflix.titus.supplementary.relocation.store.memory.InMemoryRelocationStoreModule;
import com.netflix.titus.supplementary.relocation.workflow.RelocationWorkflowModule;

public class TaskRelocationMainModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(Archaius2ConfigurationLogger.class).asEagerSingleton();
        bind(Registry.class).toInstance(new DefaultRegistry());

        install(new GovernatorJerseySupportModule());

        install(new TitusEntitySanitizerModule());
        bind(EntityValidator.class).to(PassJobValidator.class);

        install(new InMemoryRelocationStoreModule());
        install(new MasterConnectorModule());
        install(new MasterDataReplicationModule());
        install(new DeschedulerModule());
        install(new RelocationWorkflowModule());
        install(new TaskRelocationEndpointModule());
    }

    @Provides
    @Singleton
    public TitusRuntime getTitusRuntime(Registry registry) {
        CodeInvariants codeInvariants = new CompositeCodeInvariants(
                LoggingCodeInvariants.getDefault(),
                new SpectatorCodeInvariants(registry.createId("titus.runtime.invariant.violations"), registry)
        );
        return new DefaultTitusRuntime(codeInvariants, LoggingSystemLogService.getInstance(), LoggingSystemAbortListener.getDefault(), registry);
    }

    @Provides
    @Singleton
    public RelocationConfiguration getRelocationConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(RelocationConfiguration.class);
    }
}
