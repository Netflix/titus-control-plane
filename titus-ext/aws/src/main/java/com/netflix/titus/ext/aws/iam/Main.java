/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.ext.aws.iam;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementAsyncClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsync;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceAsyncClientBuilder;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.SystemExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.ext.aws.AmazonStsAsyncProvider;
import com.netflix.titus.ext.aws.AwsConfiguration;
import com.netflix.titus.ext.aws.DataPlaneAgentCredentialsProvider;

import static com.netflix.titus.common.util.CollectionsExt.asSet;

public class Main {

    private static final String REGION = "us-east-1";

    private static final Set<String> ALL_COMMANDS = asSet("agentAssume");

    private static final AwsConfiguration CONFIGURATION = Archaius2Ext.newConfiguration(AwsConfiguration.class,
            ImmutableMap.<String, String>builder()
                    .put("titus.ext.aws.region", REGION)
                    .putAll(SystemExt.getSystemPropertyMap())
                    .build()
    );

    private final AwsIamConnector connector;

    private Main(AwsIamConnector connector) {
        this.connector = connector;
    }

    private void canAgentAssume(String roleName) {
        try {
            connector.canAgentAssume(roleName).block();
        } catch (Exception e) {
            System.out.println("Agent assume role failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length == 0 || !ALL_COMMANDS.contains(args[0])) {
            helpAndExit();
        }
        String cmd = args[0];

        if (asSet("agentAssume").contains(cmd) && args.length < 2) {
            helpAndExit();
        }
        List<String> params = args.length == 1 ? Collections.emptyList() : CollectionsExt.asList(args, 1);

        Stopwatch started = Stopwatch.createStarted();
        try {
            Main main = new Main(createConnector());
            if (cmd.equals("agentAssume")) {
                main.canAgentAssume(params.get(0));
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

    private static AwsIamConnector createConnector() {
        AWSCredentialsProvider baseCredentials = new ProfileCredentialsProvider("default");
        AWSSecurityTokenServiceAsync stsClient = new AmazonStsAsyncProvider(CONFIGURATION, baseCredentials).get();
        AWSCredentialsProvider credentialsProvider = new DataPlaneAgentCredentialsProvider(CONFIGURATION, stsClient, baseCredentials).get();

        Region currentRegion = Regions.getCurrentRegion();
        if (currentRegion == null) {
            currentRegion = Region.getRegion(Regions.US_EAST_1);
        }
        return new AwsIamConnector(
                CONFIGURATION,
                AmazonIdentityManagementAsyncClientBuilder.standard()
                        .withRegion(currentRegion.getName())
                        .withCredentials(credentialsProvider)
                        .build(),
                AWSSecurityTokenServiceAsyncClientBuilder.standard()
                        .withRegion(currentRegion.getName())
                        .withCredentials(credentialsProvider)
                        .build(),
                new DefaultRegistry()
        );
    }

    private static void helpAndExit() {
        System.err.println("Usage: Main [" + ALL_COMMANDS.stream().collect(Collectors.joining(" | ")) + ']');
        System.exit(-1);
    }
}
