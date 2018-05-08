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

package com.netflix.titus.master.scheduler.constraint;

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.scheduler.ScheduledRequest;
import org.apache.mesos.Protos;

import static com.netflix.titus.common.util.CollectionsExt.first;

/**
 * A global constraint evaluator that prevents running tasks on agent nodes, on which they previously ran, and failed.
 */
@Singleton
public class GlobalTaskResubmitConstraintEvaluator implements GlobalConstraintEvaluator {

    private static final Result OK_RESULT = new Result(true, "");

    @Inject
    public GlobalTaskResubmitConstraintEvaluator() {
    }

    @Override
    public String getName() {
        return "Global Task Resubmit Constraint Evaluator";
    }

    @Override
    public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        if (!(taskRequest instanceof ScheduledRequest)) {
            return OK_RESULT;
        }
        ScheduledRequest scheduledRequest = (ScheduledRequest) taskRequest;
        V2JobMgrIntf jobMgr = scheduledRequest.getJobMgr();

        // This may happen if job is killed, but Fenzo state not cleaned yet
        if (jobMgr == null) {
            return OK_RESULT;
        }

        Set<String> badAgents = jobMgr.getExcludedAgents();
        String agentHost = findHostName(targetVM);
        if (agentHost == null || !badAgents.contains(agentHost)) {
            return OK_RESULT;
        }

        return new Result(false, "Previous failure on agent " + taskRequest.getId());
    }

    private String findHostName(VirtualMachineCurrentState targetVM) {
        Protos.Offer offer = first(targetVM.getAllCurrentOffers());
        return offer == null ? null : offer.getHostname();
    }
}
