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

package io.netflix.titus.testkit.model.job;

import io.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import io.netflix.titus.api.jobmanager.model.job.JobModel;
import io.netflix.titus.common.data.generator.DataGenerator;

import static io.netflix.titus.common.data.generator.DataGenerator.items;
import static io.netflix.titus.common.data.generator.DataGenerator.range;

/**
 */
public final class JobClustersGenerator {

    private JobClustersGenerator() {
    }

    public static DataGenerator<String> stacks() {
        return items("main", "mainvpc", "dev");
    }

    public static DataGenerator<String> details() {
        return items("GA", "canary", "prototype");
    }

    public static DataGenerator<String> versions() {
        return range(0).map(version -> String.format("v%03d", version));
    }

    public static DataGenerator<JobGroupInfo> jobGroupInfos() {
        return DataGenerator.union(
                stacks(),
                details(),
                versions(),
                (stack, detail, sequence) -> JobModel.newJobGroupInfo().withStack(stack).withDetail(detail).withSequence(sequence).build()
        );
    }
}
