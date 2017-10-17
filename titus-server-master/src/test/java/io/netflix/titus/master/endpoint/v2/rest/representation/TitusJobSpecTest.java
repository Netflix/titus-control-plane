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

package io.netflix.titus.master.endpoint.v2.rest.representation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netflix.titus.api.endpoint.v2.rest.representation.EfsMountRepresentation;
import io.netflix.titus.api.endpoint.v2.rest.representation.EfsMountRepresentation.MountPerm;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.model.SelfManagedMigrationPolicy;
import io.netflix.titus.master.RuntimeLimitValidator;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.config.MasterConfigurationConverters;
import io.netflix.titus.master.endpoint.v2.validator.DockerImageValidator;
import io.netflix.titus.master.endpoint.v2.validator.InstanceCountValidator;
import io.netflix.titus.master.endpoint.v2.validator.ResourceLimitsValidator;
import io.netflix.titus.master.endpoint.v2.validator.TitusJobSpecValidators;
import io.netflix.titus.master.endpoint.v2.validator.ValidatorConfiguration;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.netflix.titus.master.ConfigurationMockSamples.withExecutionEnvironment;
import static io.netflix.titus.master.ConfigurationMockSamples.withJobSpec;
import static io.netflix.titus.master.ConfigurationMockSamples.withSecurityGroups;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class TitusJobSpecTest {

    private final MasterConfiguration config = withExecutionEnvironment(withSecurityGroups(withJobSpec(mock(MasterConfiguration.class))));

    private final ValidatorConfiguration validatorConfiguration = mock(ValidatorConfiguration.class);

    private final List<String> defaultSecurityGroups = MasterConfigurationConverters.getDefaultSecurityGroupList(config);

    private ObjectMapper mapper;
    private TitusJobSpecValidators titusJobSpecValidators;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
        titusJobSpecValidators = new TitusJobSpecValidators(config, validatorConfiguration);
    }

    private TitusJobSpec.Builder getTitusJobSpecBuilder() {
        return new TitusJobSpec.Builder().name("test").applicationName("app1").user("me").type(TitusJobType.service).
                version("1.1").entryPoint("entry").instances(1).instancesMin(1).instancesMax(10).instancesDesired(5)
                .cpu(1.0).memory(100.0).disk(100.0).retries(0).restartOnSuccess(true).runtimeLimitSecs((long) 0).allocateIpAddress(false);
    }

    @Test
    public void testJsonWriting() throws Exception {

        try {
            TitusJobSpec jobSpec = new TitusJobSpec(
                    "jobName", "imageName", "appName", "me", TitusJobType.batch,
                    null, "1.1", "entry", false, 1, 1, 1,
                    1, 1.0, 100, 128.0, 10, null, 0, null,
                    0, true, (long) 0, false, null, defaultSecurityGroups,
                    null, null, null, null, "jgStack",
                    "jgDetail", "001", null, new SelfManagedMigrationPolicy());
            String[] secGrps = config.getDefaultSecurityGroupsList().split(",");
            Arrays.sort(secGrps);
            StringBuilder sgString = new StringBuilder();
            boolean first = true;
            for (String sg : secGrps) {
                if (!first) {
                    sgString.append(",");
                }
                sgString.append("\"").append(sg).append("\"");
                first = false;
            }
            final String expect = "{\"name\":\"jobName\",\"applicationName\":\"imageName\",\"appName\":\"appName\",\"user\":\"me\",\"type\":\"batch\",\"labels\":null,\"version\":\"1.1\",\"entryPoint\":\"entry\",\"inService\":false,\"instances\":1,\"instancesMin\":1,\"instancesMax\":1,\"instancesDesired\":1,\"cpu\":1.0,\"memory\":100.0,\"networkMbps\":128.0,\"disk\":10.0,\"ports\":null,\"gpu\":0,\"env\":null,\"retries\":0,\"restartOnSuccess\":true,\"runtimeLimitSecs\":0,\"allocateIpAddress\":false,\"iamProfile\":null,\"securityGroups\":[\"infrastructure\",\"persistence\"],\"efs\":null,\"efsMounts\":null,\"softConstraints\":null,\"hardConstraints\":null,\"jobGroupStack\":\"jgStack\",\"jobGroupDetail\":\"jgDetail\",\"jobGroupSequence\":\"001\",\"capacityGroup\":\"appName\",\"migrationPolicy\":{\"type\":\"selfManaged\"}}";
            assertEquals(expect, mapper.writeValueAsString(jobSpec));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testJsonReading() throws Exception {
        try {
            final String json = "{\"name\":\"name\",\"applicationName\":\"app\",\"user\":\"me\",\"version\":\"1.1\",\"entryPoint\":\"entry\",\"instances\":1,\"cpu\":1.0,\"memory\":100.0,\"disk\":10.0,\"ports\":null,\"env\":null,\"retries\":0,\"restartOnSuccess\":true,\"runtimeLimitSecs\":0,\"allocateIpAddress\":false,\"securityGroups\":[\"infrastructure\"]}";
            TitusJobSpec jobSpec = mapper.readValue(json, TitusJobSpec.class);
            assertEquals(TitusJobType.batch, jobSpec.getType());
            final String jsonSvc = "{\"name\":\"name\",\"applicationName\":\"app\",\"user\":\"me\",\"type\":\"service\",\"version\":\"1.1\",\"entryPoint\":\"entry\",\"instances\":1,\"cpu\":1.0,\"memory\":100.0,\"disk\":10.0,\"ports\":null,\"env\":null,\"retries\":0,\"restartOnSuccess\":true,\"runtimeLimitSecs\":0,\"allocateIpAddress\":false,\"securityGroups\":[\"infrastructure\"]}";
            assertEquals(TitusJobType.service, mapper.readValue(jsonSvc, TitusJobSpec.class).getType());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testJsonReadingWithoutSecurityGroupDefined() throws Exception {
        String json = "{\n" +
                "  \"name\": \"myJob\",\n" +
                "  \"applicationName\": \"trustybase\",\n" +
                "  \"appName\": \"myapp\",\n" +
                "  \"user\": \"swaggerUI\",\n" +
                "  \"type\": \"batch\",\n" +
                "  \"labels\": {\n" +
                "    \"source\": \"swagger IDL\"\n" +
                "  },\n" +
                "  \"version\": \"latest\",\n" +
                "  \"entryPoint\": \"sleep 10\",\n" +
                "  \"instances\": 1,\n" +
                "  \"cpu\": 1,\n" +
                "  \"memory\": 1024,\n" +
                "  \"disk\": 4096\n" +
                "}";
        TitusJobSpec jobSpec = mapper.readValue(json, TitusJobSpec.class);

        assertThat(jobSpec.getType(), is(equalTo(TitusJobType.batch)));
    }

    @Test
    public void testValidInstanceCountsSetting1() {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().build();
        assertTrue(titusJobSpecValidators.validate(tjs).isValid);
    }

    @Test
    public void testValidInstanceCountsSetting2() {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().instancesMin(0).instancesMax(0).instancesDesired(0).build();
        assertTrue(titusJobSpecValidators.validate(tjs).isValid);
    }

    @Test
    public void testInvalidInstanceCountsSetting1() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().instancesMin(10).instancesMax(1).instancesDesired(5).instances(5).build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(InstanceCountValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testInvalidInstanceCountsSetting2() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().instancesMin(1).instancesMax(10).instancesDesired(100).instances(100).build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(InstanceCountValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testBatchDefaultToOneInstance() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().type(TitusJobType.batch).build();
        assertEquals(1, tjs.getInstances());
        assertEquals(1, tjs.getInstancesMin());
        assertEquals(1, tjs.getInstancesMax());
        assertEquals(1, tjs.getInstancesDesired());
    }

    @Test
    public void testInvalidImageSpec() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().version("").build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(DockerImageValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testInvalidImageSpec2() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().applicationName("  ").build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(DockerImageValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testInvalidInstanceCountsForBatch() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().type(TitusJobType.batch).instances(10001).build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(InstanceCountValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testInvalidInstanceCountsForService() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().instancesMax(1003).instancesDesired(1002).type(TitusJobType.service).build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(InstanceCountValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testInvalidCpuSpec() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().cpu(33).build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(ResourceLimitsValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testInvalidMemorySpec() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().memory(255000).build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(ResourceLimitsValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testInvalidDiskSpec() throws Exception {
        final TitusJobSpec tjs = getTitusJobSpecBuilder().disk(705000).build();
        final TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(ResourceLimitsValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testRunningTimeLimitSpec() throws Exception {
        TitusJobSpec tjs = getTitusJobSpecBuilder().runtimeLimitSecs((long) (60 * 60)).type(TitusJobType.batch).build();
        TitusJobSpecValidators.ValidationResult validationResult = titusJobSpecValidators.validate(tjs);
        assertTrue(validationResult.isValid);

        tjs = getTitusJobSpecBuilder().runtimeLimitSecs((long) 0).type(TitusJobType.batch).build();
        validationResult = titusJobSpecValidators.validate(tjs);
        assertTrue(validationResult.isValid);

        TitusJobSpec sanitize = TitusJobSpec.sanitize(config, tjs);
        assertTrue(sanitize.getRuntimeLimitSecs().equals(config.getDefaultRuntimeLimit()));

        tjs = getTitusJobSpecBuilder().runtimeLimitSecs((long) -1).type(TitusJobType.batch).build();
        validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(RuntimeLimitValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());

        tjs = getTitusJobSpecBuilder().runtimeLimitSecs((long) (3600 * 24 * 100)).type(TitusJobType.batch).build();
        validationResult = titusJobSpecValidators.validate(tjs);
        assertFalse(validationResult.isValid);
        assertEquals(1, validationResult.failedValidators.size());
        assertEquals(RuntimeLimitValidator.class.getName(), validationResult.failedValidators.get(0).getClass().getName());
    }

    @Test
    public void testEfsMount() throws Exception {
        TitusJobSpec jobSpec = getTitusJobSpecBuilder().efs(EfsMountRepresentation.newBuilder().withEfsId("efsId").withMountPoint("/mnt").build()).build();
        TitusJobSpec sanitized = TitusJobSpec.sanitize(config, jobSpec);
        assertThat(sanitized.getEfs().getMountPerm(), is(equalTo(MountPerm.RW)));
    }

    @Test
    public void testEfsMounts() throws Exception {
        TitusJobSpec jobSpec = getTitusJobSpecBuilder().efsMounts(Collections.singletonList(EfsMountRepresentation.newBuilder().withEfsId("efsId").withMountPoint("/mnt").build())).build();
        TitusJobSpec sanitized = TitusJobSpec.sanitize(config, jobSpec);
        assertThat(sanitized.getEfsMounts().get(0).getMountPerm(), is(equalTo(MountPerm.RW)));
    }
}