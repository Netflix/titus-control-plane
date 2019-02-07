/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.runtime.endpoint.validator;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.titus.api.connector.cloud.IamConnector;
import com.netflix.titus.api.connector.cloud.noop.NoOpIamConnector;

public class JobSecurityValidatorModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(IamConnector.class).to(NoOpIamConnector.class);
    }

    @Provides
    @Singleton
    public JobSecurityValidatorConfiguration getJobSecurityValidatorConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(JobSecurityValidatorConfiguration.class);
    }
}
