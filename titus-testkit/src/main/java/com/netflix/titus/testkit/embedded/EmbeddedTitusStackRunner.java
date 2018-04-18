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

package com.netflix.titus.testkit.embedded;

import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCell;
import com.netflix.titus.testkit.embedded.cell.gateway.EmbeddedTitusGateway;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.federation.EmbeddedTitusFederation;
import com.netflix.titus.testkit.perf.load.LoadGenerator;
import org.apache.log4j.PropertyConfigurator;

public class EmbeddedTitusStackRunner {

    static {
        PropertyConfigurator.configure(LoadGenerator.class.getClassLoader().getResource("embedded-log4j.properties"));
    }

    public static void main(String[] args) throws InterruptedException {
        EmbeddedTitusMaster.Builder masterBuilder = EmbeddedTitusMaster.aTitusMaster()
                .withProperty("titus.master.grpcServer.v3EnabledApps", ".*")
                .withProperty("titus.scheduler.globalTaskLaunchingConstraintEvaluatorEnabled", "false")
                .withApiPort(8080)
                .withGrpcPort(8090);

        if (args.length > 0) {
            if (args.length != 2) {
                System.err.println("Expected cloud simulator host and port number");
                System.exit(-1);
            }
            masterBuilder.withRemoteCloud(args[0], Integer.parseInt(args[1]));
        }

        EmbeddedTitusMaster titusMaster = masterBuilder.build();

        EmbeddedTitusCell cell = EmbeddedTitusCell.aTitusCell()
                .withMaster(titusMaster)
                .withGateway(
                        EmbeddedTitusGateway.aDefaultTitusGateway()
                                .withMasterEndpoint("localhost", 8090, 8080)
                                .withHttpPort(8081)
                                .withGrpcPort(8091)
                                .build(),
                        false
                )
                .build();

        EmbeddedTitusFederation stack = EmbeddedTitusFederation.aDefaultTitusFederation()
                .withCell( ".*", cell)
                .withHttpPort(8082)
                .withGrpcPort(8092)
                .build();

        cell.boot();
        stack.boot();

        System.out.println("TitusStack started");
        Thread.sleep(24 * 60 * 60 * 1000L);
    }
}
