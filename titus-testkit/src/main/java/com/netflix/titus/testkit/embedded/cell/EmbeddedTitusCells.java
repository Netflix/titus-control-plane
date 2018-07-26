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

package com.netflix.titus.testkit.embedded.cell;

import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;

public class EmbeddedTitusCells {

    public static EmbeddedTitusCell basicCell(int desired) {
        return basicCell(EmbeddedTitusMaster.CELL_NAME, desired);
    }

    public static EmbeddedTitusCell basicCell(String cellName, int desired) {
        SimulatedCloud simulatedCloud = SimulatedClouds.basicCloud(desired);

        return EmbeddedTitusCell.aTitusCell()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud).toBuilder()
                        .withCellName(cellName)
                        .build()
                )
                .withDefaultGateway()
                .build();
    }

    public static EmbeddedTitusCell twoPartitionsPerTierCell(int desired) {
        SimulatedCloud simulatedCloud = SimulatedClouds.twoPartitionsPerTierStack(desired);

        return EmbeddedTitusCell.aTitusCell()
                .withMaster(EmbeddedTitusMasters.basicMaster(simulatedCloud))
                .withDefaultGateway()
                .build();
    }
}
