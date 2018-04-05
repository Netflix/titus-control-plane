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

package com.netflix.titus.testkit.model.v2;

import com.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public final class TitusV2ModelAsserts {

    private TitusV2ModelAsserts() {
    }

    public static void assertAllTasksInState(TitusJobInfo jobInfo, TitusTaskState expectedState) {
        int desired = jobInfo.getType() == TitusJobType.batch
                ? jobInfo.getInstances()
                : jobInfo.getInstancesDesired();
        assertThat(desired + " tasks expected", jobInfo.getTasks().size(), is(equalTo(desired)));

        jobInfo.getTasks().forEach(t -> assertTaskInState(t, expectedState));
    }

    public static void assertTaskInState(TaskInfo taskInfo, TitusTaskState expectedState) {
        assertThat(taskInfo.getId() + " not in " + expectedState + " state, but " + taskInfo.getState(),
                taskInfo.getState(),
                is(equalTo(expectedState)
                )
        );
    }
}
