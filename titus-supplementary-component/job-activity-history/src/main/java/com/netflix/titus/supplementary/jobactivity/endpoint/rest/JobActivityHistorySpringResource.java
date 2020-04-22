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

package com.netflix.titus.supplementary.jobactivity.endpoint.rest;

import com.netflix.titus.grpc.protogen.JobActivityQueryResult;
import com.netflix.titus.grpc.protogen.TaskActivityQueryResult;
import com.netflix.titus.supplementary.jobactivity.endpoint.TestData;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/api/v3/jobactivity")
public class JobActivityHistorySpringResource {

    @RequestMapping(method = RequestMethod.GET, path = "/job/{id}", produces = "application/json")
    public JobActivityQueryResult getJobActivityRecord(@PathVariable("id") String id) {
        return TestData.newJobActivityQueryResult();
    }

    @RequestMapping(method = RequestMethod.GET, path = "/task/{id}", produces = "application/json")
    public TaskActivityQueryResult getTaskActivityRecord(@PathVariable("id") String id) {
        return TestData.newTaskActivityQueryResult();
    }
}
