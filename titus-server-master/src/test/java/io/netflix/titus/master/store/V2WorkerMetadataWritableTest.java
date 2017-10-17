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

package io.netflix.titus.master.store;

import java.util.ArrayList;
import java.util.Collections;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.master.ConfigurationMockSamples;
import io.netflix.titus.master.config.MasterConfiguration;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.mock;

public class V2WorkerMetadataWritableTest {

    private ObjectMapper mapper;

    private final MasterConfiguration config = ConfigurationMockSamples.withExecutionEnvironment(mock(MasterConfiguration.class));

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        ConfigurationMockSamples.withExecutionEnvironment(config);
    }

    @Test
    public void testJsonReading() throws Exception {
        String jsonStr = "{\"workerIndex\":0,\"workerNumber\":3,\"jobId\":\"92b04137-4dc0-4e6b-8f7e-72a7bbeac932\",\"stageNum\":1,\"numberOfPorts\":1,\"metricsPort\":0,\"debugPort\":-1,\"ports\":[31000],\"state\":\"Accepted\",\"slave\":\"lgud-spodila2\",\"slaveID\":null,\"acceptedAt\":1396047646725,\"launchedAt\":1396047647688,\"startingAt\":1396047648568,\"startedAt\":1396047648594,\"completedAt\":1396047762960,\"reason\":\"Lost\",\"completionMessage\":null,\"resubmitOf\":2,\"totalResubmitCount\":3, \"latestAuditLogEvents\":[]}";
        V2WorkerMetadataWritable mwmd = mapper.readValue(jsonStr, V2WorkerMetadataWritable.class);
        final String complMsg = "Testing 123";
        mwmd.setCompletionMessage(complMsg);
        Assert.assertEquals("92b04137-4dc0-4e6b-8f7e-72a7bbeac932", mwmd.getJobId());
        Assert.assertEquals(1, mwmd.getNumberOfPorts());
        Assert.assertEquals(complMsg, mwmd.getCompletionMessage());
    }

    @Test
    public void testSettingData() throws Exception {
        String jsonStr =
                "{\"workerIndex\":0,\"workerNumber\":3,\"jobId\":\"1234\",\"workerInstanceId\":\"92b04137-4dc0-4e6b-8f7e-72a7bbeac932\"," +
                        "\"stageNum\":1,\"numberOfPorts\":1,\"metricsPort\":0,\"debugPort\":-1,\"ports\":[31000],\"state\":\"Accepted\",\"slave\":\"lgud-spodila2\"," +
                        "\"slaveID\":null,\"acceptedAt\":1396047646725,\"launchedAt\":1396047647688,\"startingAt\":1396047648568,\"startedAt\":1396047648594," +
                        "\"completedAt\":1396047762960,\"migrationDeadline\":1396047762960,\"reason\":\"Lost\",\"completionMessage\":null,\"resubmitOf\":2,\"totalResubmitCount\":3}";
        V2WorkerMetadataWritable mwmd = mapper.readValue(jsonStr, V2WorkerMetadataWritable.class);
        final String complMsg = "Testing 123";
        mwmd.setCompletionMessage(complMsg);
        mwmd.setStatusData(mapper.readValue("{\"ipAddresses\":{\"nfvpc\":\"100.66.26.69\"}}", Object.class));
        Assert.assertEquals("1234", mwmd.getJobId());
        Assert.assertEquals(1, mwmd.getNumberOfPorts());
        Assert.assertEquals(complMsg, mwmd.getCompletionMessage());

        String expectedJson =
                "{\"workerIndex\":0,\"workerNumber\":3,\"jobId\":\"1234\",\"workerInstanceId\":\"92b04137-4dc0-4e6b-8f7e-72a7bbeac932\"," +
                        "\"stageNum\":1,\"numberOfPorts\":1,\"metricsPort\":0,\"debugPort\":-1,\"ports\":[31000],\"state\":\"Accepted\",\"slave\":\"lgud-spodila2\"," +
                        "\"slaveID\":null,\"twoLevelResources\":null,\"slaveAttributes\":{},\"acceptedAt\":1396047646725,\"launchedAt\":1396047647688,\"startingAt\":1396047648568,\"startedAt\":1396047648594," +
                        "\"completedAt\":1396047762960,\"reason\":\"Lost\",\"completionMessage\":\"Testing 123\"," + "" +
                        "\"statusData\":{\"ipAddresses\":{\"nfvpc\":\"100.66.26.69\"}}," +
                        "\"resubmitOf\":2,\"totalResubmitCount\":3,\"migrationDeadline\":1396047762960}";
        Assert.assertEquals(expectedJson, mapper.writeValueAsString(mwmd));

        V2WorkerMetadataWritable m2 = mapper.readValue(expectedJson, V2WorkerMetadataWritable.class);
        Assert.assertEquals("{\"ipAddresses\":{\"nfvpc\":\"100.66.26.69\"}}", mapper.writeValueAsString(m2.getStatusData()));
        System.out.println(mapper.writeValueAsString(mwmd));

        TaskInfo taskInfo = new TaskInfo(mwmd.getJobId(), mwmd.getWorkerInstanceId(),
                TitusTaskState.FAILED,
                "host", "hostInstanceId", "itype", "region", "zone", "now", "now", "now", "now", "now", "noMsg", mwmd.getStatusData(),
                "http://stdout.live",
                "http://logs",
                "http://snapshots"
        );
        TitusJobInfo jobInfo = new TitusJobInfo(mwmd.getJobId(), "name", TitusJobType.batch, null, "app", "app",
                "me", "ver", null, false, TitusJobState.QUEUED, 1, 1, 1, 1, 1, 100, 128, 100, null, 0, null, null, null, "app",
                null, null, singletonMap("stack", "testStack"), 0, 0L, false, null, null, null, null, "now", null, null,
                Collections.singletonList(taskInfo), new ArrayList<>());

        final String expect = "{\"id\":\"1234\",\"name\":\"name\",\"type\":\"batch\",\"labels\":null,\"applicationName\":\"app\",\"appName\":\"app\",\"user\":\"me\",\"version\":\"ver\",\"entryPoint\":null,\"inService\":false,\"state\":\"QUEUED\",\"instances\":1,\"instancesMin\":1,\"instancesMax\":1,\"instancesDesired\":1,\"cpu\":1.0,\"memory\":100.0,\"networkMbps\":128.0,\"disk\":100.0,\"ports\":null,\"gpu\":0,\"jobGroupStack\":null,\"jobGroupDetail\":null,\"jobGroupSequence\":null,\"capacityGroup\":\"app\",\"migrationPolicy\":null,\"environment\":null,\"titusContext\":{\"stack\":\"testStack\"},\"retries\":0,\"runtimeLimitSecs\":0,\"allocateIpAddress\":false,\"iamProfile\":null,\"securityGroups\":null,\"efs\":null,\"efsMounts\":null,\"submittedAt\":\"now\",\"softConstraints\":null,\"hardConstraints\":null,\"tasks\":[{\"id\":\"1234\",\"instanceId\":\"92b04137-4dc0-4e6b-8f7e-72a7bbeac932\",\"state\":\"FAILED\",\"host\":\"host\",\"hostInstanceId\":\"hostInstanceId\",\"itype\":\"itype\",\"region\":\"region\",\"zone\":\"zone\",\"submittedAt\":\"now\",\"launchedAt\":\"now\",\"startedAt\":\"now\",\"finishedAt\":\"now\",\"migrationDeadline\":\"now\",\"message\":\"noMsg\",\"data\":{\"ipAddresses\":{\"nfvpc\":\"100.66.26.69\"}},\"stdoutLive\":\"http://stdout.live\",\"logs\":\"http://logs\",\"snapshots\":\"http://snapshots\"}],\"auditLogs\":[]}";
        String actual = mapper.writeValueAsString(jobInfo);
        Assert.assertEquals(expect, actual);
    }
}
