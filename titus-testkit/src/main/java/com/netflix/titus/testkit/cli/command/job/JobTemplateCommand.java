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

package com.netflix.titus.testkit.cli.command.job;

import java.io.File;
import java.io.FileWriter;
import java.util.Collections;
import java.util.Map;

import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.grpc.protogen.BatchJobSpec;
import com.netflix.titus.grpc.protogen.Capacity;
import com.netflix.titus.grpc.protogen.Constraints;
import com.netflix.titus.grpc.protogen.Container;
import com.netflix.titus.grpc.protogen.Image;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobDescriptor.JobSpecCase;
import com.netflix.titus.grpc.protogen.JobGroupInfo;
import com.netflix.titus.grpc.protogen.Owner;
import com.netflix.titus.grpc.protogen.RetryPolicy;
import com.netflix.titus.grpc.protogen.SecurityProfile;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.testkit.cli.CliCommand;
import com.netflix.titus.testkit.cli.CommandContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.CollectionsExt.asMap;
import static java.util.Arrays.asList;

/**
 * Write to file system job template.
 */
public class JobTemplateCommand implements CliCommand {

    private static final Logger logger = LoggerFactory.getLogger(JobTemplateCommand.class);

    @Override
    public String getDescription() {
        return "generate job template file";
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("t").longOpt("type").argName("name").hasArg()
                .desc("Job type, one of {BATCH, SERVICE} (defaults to 'BATCH')").build());
        options.addOption(Option.builder("f").longOpt("file").argName("file name").hasArg().required()
                .desc("Template file name").build());
        return options;
    }

    @Override
    public void execute(CommandContext context) throws Exception {
        CommandLine cli = context.getCLI();

        JobSpecCase jobCase = JobSpecCase.BATCH;
        if (cli.hasOption('t')) {
            String jobType = cli.getOptionValue('t');
            try {
                jobCase = JobSpecCase.valueOf(jobType);
            } catch (IllegalArgumentException e) {
                logger.error("Unknown job type {}. Expected one of {}", jobType, asList(JobSpecCase.BATCH, JobSpecCase.SERVICE));
                return;
            }
        }
        File templateFile = new File(cli.getOptionValue('f'));

        JobDescriptor jobDescriptor;
        switch (jobCase) {
            case SERVICE:
                jobDescriptor = createServiceJobDescriptor();
                break;
            default:
                jobDescriptor = createBatchJobDescriptor();
        }
        String formatted = JsonFormat.printer().print(jobDescriptor);

        try (FileWriter fw = new FileWriter(templateFile)) {
            fw.write(formatted);
        }

        logger.info("Generated template file in {}", templateFile.getAbsoluteFile());
    }

    private JobDescriptor createBatchJobDescriptor() {
        Container container = createContainer()
                .addAllEntryPoint(asList("echo", "\"Hello\""))
                .build();

        BatchJobSpec jobSpec = BatchJobSpec.newBuilder()
                .setRuntimeLimitSec(180)
                .setSize(1)
                .setRetryPolicy(RetryPolicy.newBuilder().setDelayed(RetryPolicy.Delayed.newBuilder().setDelayMs(1000).setRetries(3)))
                .build();

        return JobDescriptor.newBuilder()
                .setApplicationName("myBatchApp")
                .setOwner(createUser())
                .setCapacityGroup("batch_workloads")
                .setContainer(container)
                .putAllAttributes(createLabels())
                .setJobGroupInfo(createJobGroupInfo())
                .setBatch(jobSpec)
                .build();
    }

    private JobDescriptor createServiceJobDescriptor() {
        Container container = createContainer()
                .addAllEntryPoint(asList("sleep", "30"))
                .build();

        ServiceJobSpec jobSpec = ServiceJobSpec.newBuilder()
                .setCapacity(Capacity.newBuilder().setMin(1).setDesired(5).setMax(10).build())
                .setEnabled(true)
                .setRetryPolicy(RetryPolicy.newBuilder().setDelayed(RetryPolicy.Delayed.newBuilder().setDelayMs(1000).setRetries(3)))
                .build();

        return JobDescriptor.newBuilder()
                .setApplicationName("myServiceApp")
                .setOwner(createUser())
                .setCapacityGroup("service_workloads")
                .setContainer(container)
                .putAllAttributes(createLabels())
                .setJobGroupInfo(createJobGroupInfo())
                .setService(jobSpec)
                .build();
    }

    private Map<String, String> createLabels() {
        return asMap("labelA", "valueA");
    }

    private JobGroupInfo.Builder createJobGroupInfo() {
        return JobGroupInfo.newBuilder()
                .setStack("myStack")
                .setDetail("detail")
                .setSequence("001");
    }

    private Container.Builder createContainer() {
        return Container.newBuilder()
                .setImage(Image.newBuilder().setName("trustybase").setTag("latest"))
                .setResources(createResources())
                .setSecurityProfile(createSecurityProfile())
                .setSoftConstraints(createSoftConstraints())
                .setHardConstraints(createHardConstraints())
                .putAllEnv(Collections.singletonMap("MY_ENV", "myEnv"));
    }

    private com.netflix.titus.grpc.protogen.ContainerResources createResources() {
        return com.netflix.titus.grpc.protogen.ContainerResources.newBuilder()
                .setCpu(1)
                .setMemoryMB(4096)
                .setDiskMB(16384)
                .setNetworkMbps(256)
                .setAllocateIP(true)
                .build();
    }

    private SecurityProfile createSecurityProfile() {
        return SecurityProfile.newBuilder()
                .addSecurityGroups("nf.datacenter").addSecurityGroups("nf.infrastructure")
                .setIamRole("myapp-role")
                .build();
    }

    private Owner.Builder createUser() {
        return Owner.newBuilder().setTeamEmail("myteam@netflix.com");
    }

    private Constraints createHardConstraints() {
        return Constraints.newBuilder().putConstraints("ZoneBalance", "true").build();
    }

    private Constraints createSoftConstraints() {
        return Constraints.newBuilder().putConstraints("UniqueHost", "true").build();
    }
}
