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

package io.netflix.titus.ext.aws;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.autoscaling.AmazonAutoScalingAsyncClientBuilder;
import com.amazonaws.services.ec2.AmazonEC2AsyncClientBuilder;
import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.api.connector.cloud.InstanceLaunchConfiguration;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.tuple.Either;

import static io.netflix.titus.common.util.CollectionsExt.asSet;

/**
 * Helper class for manual testing of {@link AwsInstanceCloudConnector}.
 */
public class Main {

    private static final String REGION = "us-east-1";

    private static final Set<String> ALL_COMMANDS = asSet("all", "sg", "instancesByInstanceGroupId", "instance",
            "terminate", "shrink", "tag", "tagged", "reaper", "scaleUp", "scaleDown");

    private static final AwsConfiguration CONFIGURATION = new AwsConfiguration() {
        @Override
        public String getRegion() {
            return REGION;
        }

        @Override
        public long getInstanceGroupsFetchTimeoutMs() {
            return 300_000;
        }

        @Override
        public long getInstancesByInstanceGroupIdFetchTimeoutMs() {
            return 60_000;
        }

        @Override
        public long getAwsRequestTimeoutMs() {
            return 30_000;
        }

        @Override
        public long getReaperIntervalMs() {
            return 10_000;
        }
    };

    private final AwsInstanceCloudConnector connector;

    private Main(AwsInstanceCloudConnector connector) {
        this.connector = connector;
    }

    private void fetchInstanceGroups() {
        List<InstanceGroup> instanceGroups = connector.getInstanceGroups().toBlocking().first();
        System.out.println("Loaded instance groups: " + instanceGroups.size());
    }

    private void fetchInstanceGroup(List<String> ids) {
        List<InstanceGroup> instanceGroups = createConnector().getInstanceGroups(ids).toBlocking().first();
        System.out.println("Loaded instance groups: " + instanceGroups);

        List<InstanceLaunchConfiguration> launchConfigurations = createConnector().getInstanceLaunchConfiguration(instanceGroups.stream().map(g -> g.getLaunchConfigurationName()).collect(Collectors.toList())).toBlocking().first();
        System.out.println("Launch configurations: " + launchConfigurations);
    }

    private void fetchInstancesByInstanceGroupId(List<String> instanceGroupIds) {
        String instanceGroupId = instanceGroupIds.get(0);
        List<Instance> instances = connector.getInstancesByInstanceGroupId(instanceGroupId).toBlocking().first();
        System.out.println("Loaded " + instances.size() + " instances: " + instances);
    }

    private void fetchInstance(List<String> ids) {
        List<Instance> instances = connector.getInstances(ids).toBlocking().first();
        System.out.println("Loaded instances: " + instances);
    }

    private void fetchTaggedInstances(List<String> tags) {
        List<Instance> instances = connector.getTaggedInstances(tags.get(0)).toBlocking().first();
        System.out.format("Loaded tagged instances (%s):\n", instances.size());
        instances.forEach(System.out::println);
    }

    private void addTag(List<String> instanceTagValue) {
        String instanceId = instanceTagValue.get(0);
        String tag = instanceTagValue.get(1);
        String value = instanceTagValue.get(2);
        Throwable throwable = connector.addTagToResource(instanceId, tag, value).get();
        if (throwable != null) {
            throwable.printStackTrace();
        } else {
            System.out.println("Tag added");
        }
    }

    private void terminateInstances(List<String> ids, boolean shrink) {
        List<Instance> instances = connector.getInstances(ids).toBlocking().first();
        List<Either<Boolean, Throwable>> result = connector.terminateInstances(instances.get(0).getInstanceGroupId(), ids, shrink).toBlocking().first();
        for (int i = 0; i < ids.size(); i++) {
            Either<Boolean, Throwable> status = result.get(i);
            if (status.hasError()) {
                System.out.println(ids.get(i) + " termination error");
                status.getError().printStackTrace();
            } else {
                System.out.println(ids.get(i) + " terminated successfully");
            }
        }
    }

    private void runReaper() {
        InstanceReaper reaper = new InstanceReaper(CONFIGURATION, connector, new DefaultRegistry());
        reaper.enterActiveMode();
        System.out.println("Reaper process started...");

        try {
            Thread.sleep(600_000);
        } catch (InterruptedException ignore) {
        }
    }

    private void scaleUp(List<String> params) {
        String instanceGroupId = params.get(0);
        int scaleUpCount = Integer.parseInt(params.get(1));

        InstanceGroup current = connector.getInstanceGroups(Collections.singletonList(instanceGroupId)).toBlocking().first().get(0);
        System.out.println("Desired before scale-up: " + current.getDesired());

        Throwable error = connector.scaleUp(instanceGroupId, scaleUpCount).get();
        if (error != null) {
            System.err.println("Scale-up error: " + error.getMessage());
            error.printStackTrace();
        }

        InstanceGroup after = connector.getInstanceGroups(Collections.singletonList(instanceGroupId)).toBlocking().first().get(0);
        System.out.println("Desired after scale-up: " + after.getDesired());
    }

    private void scaleDown(List<String> params) {
        String instanceGroupId = params.get(0);
        int scaleDownCount = Integer.parseInt(params.get(1));

        InstanceGroup current = connector.getInstanceGroups(Collections.singletonList(instanceGroupId)).toBlocking().first().get(0);
        System.out.println("Desired before scale-down: " + current.getDesired());

        Throwable error = connector.updateCapacity(instanceGroupId, Optional.empty(), Optional.of(current.getDesired() - scaleDownCount)).get();
        if (error != null) {
            System.err.println("Scale-down error: " + error.getMessage());
            error.printStackTrace();
        }

        InstanceGroup after = connector.getInstanceGroups(Collections.singletonList(instanceGroupId)).toBlocking().first().get(0);
        System.out.println("Desired after scale-down: " + after.getDesired());
    }

    public static void main(String[] args) {
        if (args.length == 0 || !ALL_COMMANDS.contains(args[0])) {
            helpAndExit();
        }
        String cmd = args[0];

        if (asSet("all", "reaper").contains(cmd) && args.length != 1) {
            helpAndExit();
        }
        if (asSet("sg", "instance", "terminate", "shrink", "tagged", "scaleUp").contains(cmd) && args.length < 2) {
            helpAndExit();
        }
        if (asSet("tag").contains(cmd) && args.length != 4) {
            helpAndExit();
        }
        List<String> params = args.length == 1 ? Collections.emptyList() : CollectionsExt.asList(args, 1);

        Stopwatch started = Stopwatch.createStarted();
        try {
            Main main = new Main(createConnector());
            if (cmd.equals("all")) {
                main.fetchInstanceGroups();
            } else if (cmd.equals("sg")) {
                main.fetchInstanceGroup(params);
            } else if (cmd.equals("instancesByInstanceGroupId")) {
                main.fetchInstancesByInstanceGroupId(params);
            } else if (cmd.equals("instance")) {
                main.fetchInstance(params);
            } else if (cmd.equals("terminate")) {
                main.terminateInstances(params, false);
            } else if (cmd.equals("shrink")) {
                main.terminateInstances(params, true);
            } else if (cmd.equals("tagged")) {
                main.fetchTaggedInstances(params);
            } else if (cmd.equals("tag")) {
                main.addTag(params);
            } else if (cmd.equals("reaper")) {
                main.runReaper();
            } else if (cmd.equals("scaleUp")) {
                main.scaleUp(params);
            } else if (cmd.equals("scaleDown")) {
                main.scaleDown(params);
            }
        } catch (Throwable e) {
            e.printStackTrace();
            doExit(started, -1);
        } finally {
            doExit(started, 0);
        }
    }

    private static void doExit(Stopwatch started, int status) {
        System.out.println("Finished in " + started.elapsed(TimeUnit.SECONDS) + "sec");
        createConnector().shutdown();
        System.exit(status);
    }

    private static AwsInstanceCloudConnector createConnector() {
        EnvironmentVariableCredentialsProvider credentialsProvider = new EnvironmentVariableCredentialsProvider();
        Region currentRegion = Regions.getCurrentRegion();
        if (currentRegion == null) {
            currentRegion = Region.getRegion(Regions.US_EAST_1);
        }
        return new AwsInstanceCloudConnector(
                CONFIGURATION,
                AmazonEC2AsyncClientBuilder.standard()
                        .withRegion(currentRegion.getName())
                        .withCredentials(credentialsProvider)
                        .build(),
                AmazonAutoScalingAsyncClientBuilder.standard()
                        .withRegion(currentRegion.getName())
                        .withCredentials(credentialsProvider)
                        .build()
        );
    }

    private static void helpAndExit() {
        System.err.println("Usage: Main [" + ALL_COMMANDS.stream().collect(Collectors.joining(" | ")) + ']');
        System.exit(-1);
    }
}
