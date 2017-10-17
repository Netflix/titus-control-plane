/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.embedded.stack;

import io.netflix.titus.testkit.embedded.gateway.EmbeddedTitusGateway;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;

public class EmbeddedTitusStackRunner {

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 2 && args.length != 5) {
            System.err.println("ERROR: provide jobFile, stageFile, workerFile, agentFile, and tierFile");
            return;
        }

        EmbeddedTitusMaster.Builder masterBuilder = EmbeddedTitusMaster.aTitusMaster()
                .withProperty("mantis.master.grpcServer.v3EnabledApps", "v3App")
                .withApiPort(8080)
                .withGrpcPort(8090);
//        if (args.length == 5) {
//            masterBuilder.withJobDataFromFile(args[0], args[1], args[2])
//                    .withAgentClusterFileConfig(args[3])
//                    .withTierConfigFile(args[4]);
//        } else {
//            masterBuilder.withAgentClusterFileConfig(args[0])
//                    .withTierConfigFile(args[1]);
//        }
        EmbeddedTitusMaster titusMaster = masterBuilder.build();

        EmbeddedTitusStack stack = EmbeddedTitusStack.aTitusStack()
                .withMaster(titusMaster)
                .withGateway(
                        EmbeddedTitusGateway.aDefaultTitusGateway()
                                .withMasterGrpcEndpoint("localhost", 8090)
                                .withHttpPort(8081)
                                .build()
                )
                .build();

        stack.boot();
        System.out.println("TitusStack started");
        Thread.sleep(24 * 60 * 60 * 1000L);
    }
}
