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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netflix.titus.api.model.v2.JobOwner;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.NamedJobDefinition;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.audit.service.AuditLogService;
import io.netflix.titus.master.config.MasterConfiguration;
import junit.framework.Assert;
import junit.framework.TestCase;
import rx.subjects.ReplaySubject;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NamedJobsTest extends TestCase {
    private static final NamedJobs namedJobs;
    private static final String name = "fooJob";
    private static final List<Parameter> params = new ArrayList<>();

    private static final MasterConfiguration config = mock(MasterConfiguration.class);

    private static final AuditLogService auditLogWriterMock = mock(AuditLogService.class);

    static {
        when(config.getMaximumNumberOfJarsPerJobName()).thenReturn(10);

        namedJobs = new NamedJobs(config, new ConcurrentHashMap<>(), ReplaySubject.create(), new ConcurrentHashMap<>(), null, null, auditLogWriterMock);
        namedJobs.init(new NoopStorageProvider());
        URL jarUrl = null;
        try {
            jarUrl = new URL("file:///tmp/foobar.jar");
        } catch (MalformedURLException e) {
            Assert.fail(e.getMessage());
        }
        final String version = "1.0";
        params.add(new Parameter("param1", "val1"));
        params.add(new Parameter("param2", "val2"));
        Map<Integer, StageSchedulingInfo> stages = new HashMap<>();
        StageSchedulingInfo stageSchedulingInfo = new StageSchedulingInfo(1,
                new MachineDefinition(1, 10, 10, 10, 1, null), null, null, null, false, null, false);
        stages.put(1, stageSchedulingInfo);
        SchedulingInfo schedulingInfo = new SchedulingInfo(stages);
        V2JobDefinition jobDefinition = new V2JobDefinition(name, "spo", jarUrl, version, params, null, 0, schedulingInfo, 1, 1, null, null);
        V2JobDefinition jobDefinition2 = new V2JobDefinition("new_" + name, "spo", jarUrl, version, params, null, 0, schedulingInfo, 1, 1, null, null);
        try {
            namedJobs.createNamedJob(new NamedJobDefinition(jobDefinition, new JobOwner("spo", "Mantis", "", null, null)));
            namedJobs.createNamedJob(new NamedJobDefinition(jobDefinition2, new JobOwner("spo", "Mantis", "", null, null)));
        } catch (InvalidNamedJobException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    public void testGetResolvedJobDefinitionInheritParams() throws Exception {
        final V2JobDefinition resolved = namedJobs.getResolvedJobDefinition(new V2JobDefinition(name, "foo", null, null, null, null, 0, null, 2, 2, null, null));
        final List<Parameter> resolvedParameters = resolved.getParameters();
        Assert.assertEquals("#parameters", params.size(), resolvedParameters.size());
        Assert.assertEquals("Parameter 1", "param1", resolvedParameters.get(0).getName());
        Assert.assertEquals("Parameter 2", "param2", resolvedParameters.get(1).getName());
        Assert.assertEquals("Value 1", "val1", resolvedParameters.get(0).getValue());
        Assert.assertEquals("Value 2", "val2", resolvedParameters.get(1).getValue());
    }

    public void testUpdateNonExistingJob() throws Exception {
        try {
            namedJobs.updateNamedJob(new NamedJobDefinition(new V2JobDefinition("notthere", "spo", null, null, null, null, 0, null, 1, 1, null, null), null), false);
            Assert.fail("Unexpected to update non-existing named job");
        } catch (InvalidNamedJobException e) {
        }
    }

    public void testUpdateJobInheritParams() throws Exception {
        try {
            final String newVersion = "" + System.currentTimeMillis();
            final NamedJob created = namedJobs.updateNamedJob(new NamedJobDefinition(
                            new V2JobDefinition(name, "spo", new URL("http://foo.com/bar.jar"),
                                    newVersion, null, null, 0, null, 1, 1, null, null), null),
                    false);
            final List<Parameter> parameters = created.getParameters();
            Assert.assertNotNull("Inherited parameters", parameters);
            Assert.assertEquals("#parameters", params.size(), parameters.size());
            final List<NamedJob.Jar> jars = created.getJars();
            Assert.assertEquals("New version", jars.get(jars.size() - 1).getVersion(), newVersion);
        } catch (InvalidNamedJobException e) {
            Assert.fail(e.getMessage());
        }
    }

    public void testUpdateJobNewParams() throws Exception {
        try {
            List<Parameter> newParams = new ArrayList<>();
            newParams.add(new Parameter("param3", "val3"));
            final NamedJob updated = namedJobs.updateNamedJob(new NamedJobDefinition(
                            new V2JobDefinition("new_" + name, "spo", new URL("http://foo.com/bar.jar"),
                                    null, newParams, null, 0, null, 1, 1, null, null), null),
                    false);
            Assert.assertEquals("#Parameters", newParams.size(), updated.getParameters().size());
        } catch (InvalidNamedJobException e) {
            Assert.fail(e.getMessage());
        }
    }
}