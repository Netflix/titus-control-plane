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

package com.netflix.titus.testkit.model.job;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Image;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.SecurityProfile;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.common.util.tuple.Triple;

import static com.netflix.titus.common.data.generator.DataGenerator.concatenations;
import static com.netflix.titus.common.data.generator.DataGenerator.items;
import static com.netflix.titus.common.data.generator.DataGenerator.merge;
import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static com.netflix.titus.common.data.generator.DataGenerator.union;
import static com.netflix.titus.common.data.generator.DataGenerator.zip;
import static com.netflix.titus.common.util.CollectionsExt.asMap;
import static com.netflix.titus.testkit.model.PrimitiveValueGenerators.hexValues;
import static com.netflix.titus.testkit.model.PrimitiveValueGenerators.semanticVersions;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public final class ContainersGenerator {

    private ContainersGenerator() {
    }

    public static DataGenerator<String> imageNames() {
        return concatenations("/",
                items("platform", "security", "applications"),
                items("webUI", "backend", "database")
        );
    }

    public static DataGenerator<String> imageTags() {
        return merge(items("latest"), semanticVersions(3));
    }

    public static DataGenerator<String> imageDigests() {
        return hexValues(32).map(hex -> "sha256:" + hex);
    }

    public static DataGenerator<Image> images() {
        return zip(imageNames(), zip(imageTags(), imageDigests())).map(v ->
                JobModel.newImage()
                        .withName(v.getLeft())
                        .withTag(v.getRight().getLeft())
                        //.withDigest(v.getRight().getRight()) digests not supported yet
                        .build()
        );
    }

    public static DataGenerator<ContainerResources> resources() {
        return union(range(1, 16), items(1, 2, 4), (cpu, ratio) ->
                ContainerResources.newBuilder()
                        .withCpu(cpu)
                        .withMemoryMB((int) (cpu * ratio * 1024))
                        .withDiskMB((int) (cpu * ratio * 2 * 10_000))
                        .withNetworkMbps((int) (cpu * ratio * 128))
                        .witShmMB((int) (cpu * ratio * 1024 / 8))
                        .build()
        );
    }

    public static DataGenerator<EfsMount> efsMounts() {
        return range(1).map(id -> EfsMount.newBuilder()
                .withEfsId("efs#" + id)
                .withMountPerm(EfsMount.MountPerm.RW)
                .withMountPoint("/data")
                .withEfsRelativeMountPoint("/home/mydata")
                .build()
        );
    }

    public static DataGenerator<SecurityProfile> securityProfiles() {
        return DataGenerator.union(
                hexValues(8).map(sg -> "sg-" + sg).batch(2),
                hexValues(8).map(iam -> "iam-" + iam),
                (sgs, iam) -> SecurityProfile.newBuilder().withSecurityGroups(sgs).withIamRole(iam).build()
        );
    }

    public static DataGenerator<Pair<Map<String, String>, Map<String, String>>> constraints() {
        return DataGenerator.<Map<String, String>, Map<String, String>>zip(
                items(
                        asMap(),
                        asMap("UniqueHost", "true")
                ),
                items(
                        asMap(),
                        asMap("ZoneBalance", "true")
                )
        ).loop();
    }

    private static DataGenerator<Triple<List<String>, List<String>, Map<String, String>>> executable() {
        DataGenerator<Pair<List<String>, List<String>>> entryPointAndCommands = DataGenerator.zip(
                items(asList("/bin/sh", "-c"), Collections.<String>emptyList()),
                items(singletonList("echo 'Hello'"), Collections.<String>emptyList())
        );
        DataGenerator<Map<String, String>> envs = items(
                asMap("ENV_A", "123"), asMap("ENV_B", "345")
        );
        return union(entryPointAndCommands, envs, (ec, e) -> Triple.of(ec.getLeft(), ec.getRight(), e)).loop();
    }

    public static DataGenerator<Container> containers() {
        return DataGenerator.bindBuilder(JobModel::newContainer)
                .bind(images(), Container.Builder::withImage)
                .bind(resources(), Container.Builder::withContainerResources)
                .bind(securityProfiles(), Container.Builder::withSecurityProfile)
                .bind(constraints(), (builder, pair) -> builder
                        .withHardConstraints(pair.getLeft())
                        .withSoftConstraints(pair.getRight()))
                .bind(executable(), (builder, ex) -> builder
                        .withEntryPoint(ex.getFirst())
                        .withCommand(ex.getSecond())
                        .withEnv(ex.getThird()))
                .map(builder -> builder
                        .withAttributes(Collections.singletonMap("labelA", "valueA"))
                        .build());
    }
}
