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
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeCluster;
import com.netflix.titus.testkit.embedded.kube.EmbeddedKubeClusters;

public class EmbeddedTitusCells {

    public static EmbeddedTitusCell basicKubeCell(int desired) {
        return basicKubeCell(EmbeddedTitusMaster.CELL_NAME, desired);
    }

    public static EmbeddedTitusCell basicKubeCellWithCustomZones(int desired, String... zones) {
        return basicKubeCell(EmbeddedTitusMaster.CELL_NAME, EmbeddedKubeClusters.basicClusterWithCustomZones(desired, zones));
    }

    public static EmbeddedTitusCell basicKubeCell(String cellName, int desired) {
        return basicKubeCell(cellName, EmbeddedKubeClusters.basicCluster(desired));
    }

    private static EmbeddedTitusCell basicKubeCell(String cellName, EmbeddedKubeCluster embeddedKubeCluster) {
        return EmbeddedTitusCell.aTitusCell()
                .withMaster(EmbeddedTitusMasters.basicMasterWithKubeIntegration(embeddedKubeCluster).toBuilder()
                        .withCellName(cellName)
                        .build()
                )
                .withDefaultGateway()
                .build();
    }
}
