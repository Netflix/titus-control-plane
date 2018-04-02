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

package com.netflix.titus.testkit.embedded.cloud;

import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import com.netflix.titus.testkit.perf.load.LoadGenerator;
import org.apache.log4j.PropertyConfigurator;

public class SimulatedCloudRunner {

    static {
        PropertyConfigurator.configure(LoadGenerator.class.getClassLoader().getResource("cloud-log4j.properties"));
    }

    public static void main(String[] args) throws InterruptedException {
        LifecycleInjector injector = InjectorBuilder.fromModule(new EmbeddedCloudModule()).createInjector();

        SimulatedCloud simulatedCloud = injector.getInstance(SimulatedCloud.class);

        simulatedCloud.createAgentInstanceGroups(
                SimulatedAgentGroupDescriptor.awsInstanceGroup("critical1", AwsInstanceType.M4_4XLarge, 5),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flex1", AwsInstanceType.R4_8XLarge, 5),
                SimulatedAgentGroupDescriptor.awsInstanceGroup("flexGpu", AwsInstanceType.G2_8XLarge, 5)
        );

        injector.awaitTermination();
    }
}
