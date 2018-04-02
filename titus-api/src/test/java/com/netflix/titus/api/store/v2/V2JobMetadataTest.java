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

package com.netflix.titus.api.store.v2;

import java.net.URL;
import java.util.LinkedList;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.model.v2.JobSla;
import com.netflix.titus.api.model.v2.V2JobDurationType;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.master.store.V2JobMetadataWritable;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

public class V2JobMetadataTest {

    ObjectMapper mapper;

    @Before
    public void setUp() throws Exception {
        mapper = new ObjectMapper();
    }

    @Test
    public void testJsonWriting() throws Exception {
        try {
            long now = System.currentTimeMillis();
            V2JobMetadataWritable mjmd = new V2JobMetadataWritable("1234", "testJob", "NoUser", now, new URL("http://nothere.com/"), 2,
                    new JobSla(0, 0L, 0L, JobSla.StreamSLAType.Lossy, V2JobDurationType.Perpetual, ""), null,
                    0L, new LinkedList<Parameter>(), 1);
            //System.out.println(mapper.writeValueAsString(mjmd));
            mjmd.setJobState(V2JobState.Failed);
            String expectedVal = "{\"jobId\":\"1234\",\"name\":\"testJob\",\"user\":\"NoUser\",\"submittedAt\":" + now + ",\"jarUrl\":\"http://nothere.com/\",\"numStages\":2,\"sla\":{\"retries\":0,\"runtimeLimitSecs\":0,\"minRuntimeSecs\":0,\"slaType\":\"Lossy\",\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"},\"state\":\"Failed\",\"subscriptionTimeoutSecs\":0,\"parameters\":[],\"nextWorkerNumberToUse\":1,\"latestAuditLogEvents\":[]}";
            //System.out.println(mapper.writeValueAsString(mjmd));
            Assert.assertEquals(expectedVal, mapper.writeValueAsString(mjmd));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testJsonReading() throws Exception {
        String jsonStr = "{\"jobId\":\"13\",\"name\":\"testJob2\",\"submittedAt\":12345,\"jarUrl\":\"http://nothere.com/foo\",\"numStages\":1,\"sla\":{\"runtimeLimitSecs\":0,\"slaType\":\"Lossy\",\"durationType\":\"Perpetual\",\"userProvidedType\":\"\"},\"parameters\":[],\"state\":\"Accepted\"}";
        V2JobMetadataWritable mjmd2 = mapper.readValue(jsonStr, V2JobMetadataWritable.class);
        Assert.assertEquals("13", mjmd2.getJobId());
        Assert.assertEquals("testJob2", mjmd2.getName());
        Assert.assertEquals(12345, mjmd2.getSubmittedAt());
        Assert.assertEquals("http://nothere.com/foo", mjmd2.getJarUrl().toString());
        Assert.assertEquals(1, mjmd2.getNumStages());
        Assert.assertEquals(JobSla.StreamSLAType.Lossy, mjmd2.getSla().getSlaType());
        Assert.assertEquals(V2JobDurationType.Perpetual, mjmd2.getSla().getDurationType());
        Assert.assertEquals(V2JobState.Accepted, mjmd2.getState());
        Assert.assertEquals(0, mjmd2.getLatestAuditLogEvents().size());
    }
}
