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

package com.netflix.titus.master.endpoint.v2;

import java.util.UUID;

import com.google.common.base.Preconditions;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import com.netflix.titus.api.model.v2.ServiceJobProcesses;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.master.ApiOperations;
import com.netflix.titus.master.config.CellInfoResolver;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.endpoint.EndpointModelAsserts;
import com.netflix.titus.master.endpoint.TitusDataGenerator;
import com.netflix.titus.master.endpoint.TitusServiceGateway;
import com.netflix.titus.master.endpoint.TitusServiceGatewayTestCompatibilityTestSuite;
import com.netflix.titus.master.endpoint.common.ContextResolver;
import com.netflix.titus.master.endpoint.common.EmptyContextResolver;
import com.netflix.titus.master.endpoint.v2.rest.RestConfig;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.master.job.JobUpdateException;
import com.netflix.titus.master.job.V2JobOperations;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.store.V2JobMetadataWritable;
import com.netflix.titus.master.store.V2StageMetadataWritable;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.common.LogStorageInfo;
import org.junit.Before;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V2LegacyTitusServiceGatewayTest extends TitusServiceGatewayTestCompatibilityTestSuite<
        String, TitusJobSpec, TitusJobType, TitusJobInfo, TitusTaskInfo, TitusTaskState> {

    private String cellName;
    private V2TitusDataGenerator dataGenerator;

    private final V2EndpointModelAsserts modelAsserts = new V2EndpointModelAsserts();

    private final MasterConfiguration config = mock(MasterConfiguration.class);
    private final RestConfig restConfig = mock(RestConfig.class);

    private final V2JobOperations v2JobOperations = mock(V2JobOperations.class);

    private final JobSubmitLimiter jobSubmitLimiter = mock(JobSubmitLimiter.class);

    private final ApiOperations apiOperations = mock(ApiOperations.class);

    private final ApplicationSlaManagementService applicationSlaManagementService = mock(ApplicationSlaManagementService.class);

    private final SchedulingService schedulingService = mock(SchedulingService.class);

    private final CellInfoResolver cellInfoResolver = mock(CellInfoResolver.class);

    private final LogStorageInfo logStorageInfo = EmptyLogStorageInfo.INSTANCE;

    private final ContextResolver contextResolver = EmptyContextResolver.INSTANCE;

    private V2LegacyTitusServiceGateway gateway;

    @Before
    public void setUp() throws Exception {
        gateway = new V2LegacyTitusServiceGateway(config, restConfig, v2JobOperations, jobSubmitLimiter, apiOperations,
                applicationSlaManagementService, schedulingService, contextResolver, cellInfoResolver, logStorageInfo);

        cellName = UUID.randomUUID().toString();
        dataGenerator = new V2TitusDataGenerator(cellName);
        when(cellInfoResolver.getCellName()).thenReturn(cellName);

        when(v2JobOperations.submit(any())).then(c -> {
            V2JobDefinition mjd = (V2JobDefinition) c.getArguments()[0];
            String name = Parameters.getName(mjd.getParameters());

            if (name.contains(OK_JOB)) {
                V2JobMetadata v2Job = dataGenerator.runtime().newJobMetadata(mjd);
                String jobId = v2Job.getJobId();
                dataGenerator.runtime().scheduleJob(jobId);

                return jobId;
            }

            throw new RuntimeException("simulated job submit error");
        });

        when(apiOperations.getAllJobsMetadata(anyBoolean(), anyInt())).thenAnswer(i -> {
            boolean activeOnly = (Boolean) i.getArguments()[0];
            return dataGenerator.runtime().getAllJobsMetadata(!activeOnly);
        });

        when(apiOperations.getJobMetadata(any())).thenAnswer(i -> {
            String jobId = (String) i.getArguments()[0];
            return dataGenerator.runtime().getJob(jobId);
        });

        when(apiOperations.killJob(any(), any())).thenAnswer(i -> {
            String jobId = (String) i.getArguments()[0];
            if (dataGenerator.runtime().getJob(jobId) == null) {
                return false;
            }
            dataGenerator.runtime().killJob(jobId);
            return true;
        });

        when(apiOperations.getWorker(any(), anyInt(), anyBoolean())).thenAnswer(i -> {
            String jobId = (String) i.getArguments()[0];
            int workerNumber = (Integer) i.getArguments()[1];

            V2JobMetadata job = dataGenerator.runtime().getJob(jobId);
            Preconditions.checkNotNull(job, "Job not found " + jobId);

            for (V2WorkerMetadata worker : job.getStageMetadata(1).getAllWorkers()) {
                if (worker.getWorkerNumber() == workerNumber) {
                    return worker;
                }
            }
            return null;
        });

        doAnswer(i -> {
            String jobId = (String) i.getArguments()[0];
            int min = (Integer) i.getArguments()[2];
            int desired = (Integer) i.getArguments()[3];
            int max = (Integer) i.getArguments()[4];

            V2JobMetadata job = dataGenerator.runtime().getJob(jobId);
            if (job == null) {
                throw new InvalidJobException("no job " + jobId);
            }

            V2StageMetadataWritable stage = (V2StageMetadataWritable) job.getStageMetadata(1);
            StageScalingPolicy current = stage.getScalingPolicy();
            stage.setScalingPolicy(new StageScalingPolicy(1,
                    min, max, desired,
                    current.getIncrement(), current.getDecrement(), current.getCoolDownSecs(), current.getStrategies()
            ));
            ServiceJobProcesses jobProcesses = stage.getJobProcesses();
            if ((jobProcesses.isDisableDecreaseDesired() && desired < current.getDesired()) ||
                    (jobProcesses.isDisableIncreaseDesired() && desired > current.getDesired())) {
                throw new JobUpdateException(String.format("Invalid desired capacity %s for jobId = %s with " +
                        "current job processes %s", desired, jobId, jobProcesses));
            }

            return null;
        }).when(apiOperations).updateInstanceCounts(any(), anyInt(), anyInt(), anyInt(), anyInt(), any());

        doAnswer(i -> {
            String jobId = (String) i.getArguments()[0];
            boolean inService = (Boolean) i.getArguments()[2];

            V2JobMetadataWritable job = (V2JobMetadataWritable) dataGenerator.runtime().getJob(jobId);
            if (job == null) {
                throw new InvalidJobException("no job " + jobId);
            }
            if (Parameters.getJobType(job.getParameters()) != Parameters.JobType.Service) {
                throw new UnsupportedOperationException(jobId + " is not service job");
            }

            job.setParameters(Parameters.updateParameter(job.getParameters(), Parameters.newInServiceParameter(inService)));

            return null;
        }).when(apiOperations).updateInServiceStatus(any(), anyInt(), anyBoolean(), any());

        doAnswer(i -> {
            String jobId = (String) i.getArguments()[0];
            int stageNum = (Integer) i.getArguments()[1];
            boolean disableIncreaseDesired = (Boolean) i.getArguments()[2];
            boolean disableDecreaseDesired = (Boolean) i.getArguments()[3];

            V2JobMetadata job = dataGenerator.runtime().getJob(jobId);
            if (job == null) {
                throw new InvalidJobException("no job " + jobId);
            }

            V2StageMetadataWritable stage = (V2StageMetadataWritable) job.getStageMetadata(stageNum);
            ServiceJobProcesses serviceJobProcesses = ServiceJobProcesses.newBuilder().withDisableDecreaseDesired(disableDecreaseDesired)
                    .withDisableIncreaseDesired(disableIncreaseDesired).build();
            stage.setJobProcesses(serviceJobProcesses);

            return null;
        }).when(apiOperations).updateJobProcesses(any(), anyInt(), anyBoolean(), anyBoolean(), any());

        super.setUp();
    }

    @Override
    protected TitusDataGenerator<String, TitusJobSpec, TitusJobType, TitusJobInfo, TitusTaskInfo, TitusTaskState> createDataGenerator() {
        return dataGenerator;
    }

    @Override
    protected EndpointModelAsserts<String, TitusJobSpec, TitusJobType, TitusJobInfo, TitusTaskInfo, TitusTaskState> createModelAsserts() {
        return modelAsserts;
    }

    @Override
    protected TitusServiceGateway<String, TitusJobSpec, TitusJobType, TitusJobInfo, TitusTaskInfo, TitusTaskState> createGateway() {
        return gateway;
    }

    @Override
    protected String getCellName() {
        return cellName;
    }
}