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
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeClusters;
import com.netflix.titus.testkit.util.cli.CommandLineBuilder;
import com.netflix.titus.testkit.util.cli.CommandLineFacade;
import org.apache.commons.cli.Option;
import org.apache.log4j.PropertyConfigurator;

public class EmbeddedTitusStackRunner {

    static {
        PropertyConfigurator.configure(EmbeddedTitusStackRunner.class.getClassLoader().getResource("embedded-log4j.properties"));
    }

    public static void main(String[] args) throws InterruptedException {
        CommandLineFacade cliFacade = buildCliFacade(args);

        EmbeddedTitusMaster titusMaster = EmbeddedTitusMaster.aTitusMaster()
                .withEmbeddedKubeCluster(EmbeddedKubeClusters.basicCluster(2))
                .withApiPort(8080)
                .withGrpcPort(7104)
                .withEnableDisruptionBudget(true)
                .build();

        boolean federationEnabled = cliFacade.isEnabled("f");

        EmbeddedTitusCell cell = EmbeddedTitusCell.aTitusCell()
                .withMaster(titusMaster)
                .withGateway(
                        EmbeddedTitusGateway.aDefaultTitusGateway()
                                .withMasterEndpoint("localhost", 8090, 8080)
                                .withHttpPort(8081)
                                .withGrpcPort(8091)
                                // Binds to the default TaskRelocation service configuration
                                // TODO Make it configurable via command line parameter
                                .withProperty("titus.relocation.grpcClient.grpcPort", "7105")
                                .withProperty("titus.feature.mergingTaskMigrationPlanInGatewayEnabled", "true")
                                .build(),
                        !federationEnabled
                )
                .build();

        cell.boot();

        if (federationEnabled) {
            EmbeddedTitusFederation stack = EmbeddedTitusFederation.aDefaultTitusFederation()
                    .withCell(".*", cell)
                    .withHttpPort(8082)
                    .withGrpcPort(8092)
                    .build();

            stack.boot();
        }

        System.out.println("TitusStack started");
        Thread.sleep(24 * 60 * 60 * 1000L);
    }

    private static CommandLineFacade buildCliFacade(String[] args) {
        CommandLineFacade cliFacade = CommandLineBuilder.newApacheCli()
                .withHostAndPortOption("Simulator/GRPC")
                .withOption(Option.builder("f").longOpt("federation").argName("federation_enabled").hasArg(false)
                        .desc("Run TitusFederation")
                        .build()
                )
                .build(args);

        if (cliFacade.hasHelpOption()) {
            cliFacade.printHelp("EmbeddedTitusStackRunner");
            System.exit(-1);
        }

        return cliFacade;
    }
}
