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

package com.netflix.titus.testkit.model.supervisor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.MasterStatus;

public class MasterInstanceGenerator {

    public static DataGenerator<MasterInstance> masterInstances(MasterState initialState, String... ids) {
        MasterStatus masterStatus = MasterStatus.newBuilder()
                .withState(initialState)
                .withReasonCode("initialValueInTest")
                .withReasonMessage("Initial value")
                .build();

        List<MasterInstance> values = new ArrayList<>();
        for (int i = 0; i < ids.length; i++) {
            values.add(
                    MasterInstance.newBuilder()
                            .withInstanceId(ids[i])
                            .withIpAddress("1.0.0." + i)
                            .withStatus(masterStatus)
                            .withStatusHistory(Collections.emptyList())
                            .build()
            );
        }

        return DataGenerator.items(values);
    }

    public static MasterInstance getLocalMasterInstance(MasterState state) {
        return masterInstances(state, "localMaster").getValue();
    }

    public static MasterInstance moveTo(MasterInstance current, MasterState state) {
        return current.toBuilder()
                .withStatus(MasterStatus.newBuilder()
                        .withState(state)
                        .withReasonCode("testTransition")
                        .withReasonMessage("Requested by test")
                        .build()
                )
                .build();
    }
}
