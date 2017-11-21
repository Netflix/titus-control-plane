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

package io.netflix.titus.master.config;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import io.netflix.titus.master.CoreConfiguration;

public interface MasterConfiguration extends CoreConfiguration {
    @PropertyName(name = "region")
    String getRegion();

    @PropertyName(name = "titus.master.apiProxyPort")
    @DefaultValue("7001")
    int getApiProxyPort();

    @PropertyName(name = "titus.master.api.status.path")
    @DefaultValue("/api/v2/jobs/heartbeat")
    String getApiStatusUri();

    @PropertyName(name = "titus.master.host")
    @DefaultValue("")
    String getMasterHost();

    @PropertyName(name = "titus.master.ip")
    @DefaultValue("")
    String getMasterIP();

    @PropertyName(name = "titus.master.job.use.resource.network.mbps")
    @DefaultValue("true")
    boolean getUseNetworkMbpsAttribute();

    @PropertyName(name = "titus.master.job.security.groups.assignment.disable")
    @DefaultValue("false")
    boolean getDisableSecurityGroupsAssignments();

    @PropertyName(name = "mesos.master.location")
    @DefaultValue("localhost:5050")
    String getMasterLocation();

    @PropertyName(name = "mesos.titus.executor")
    @DefaultValue("/apps/titus-executor/bin/run")
    String pathToTitusExecutor();

    @PropertyName(name = "titus.master.active.slave.attribute.name")
    @DefaultValue("asg")
    String getActiveSlaveAttributeName();

    @PropertyName(name = "titus.master.framework.name")
    @DefaultValue("TitusFramework")
    String getMesosFrameworkName();

    @PropertyName(name = "titus.master.mesos.failover.timeout.secs")
    @DefaultValue("604800.0")
        // 604800 secs = 1 week
    double getMesosFailoverTimeOutSecs();

    @PropertyName(name = "mesos.task.reconciliation.interval.secs")
    @DefaultValue("300")
    long getMesosTaskReconciliationIntervalSecs();

    @PropertyName(name = "mesos.lease.offer.expiry.secs")
    @DefaultValue("300")
    long getMesosLeaseOfferExpirySecs();

    @PropertyName(name = "titus.jobs.max.jars.per.named.job")
    @DefaultValue("10")
    int getMaximumNumberOfJarsPerJobName();

    @PropertyName(name = "titus.named.job.store.completed.jobs")
    @DefaultValue("false")
    boolean getStoreCompletedJobsForNamedJob();

    @PropertyName(name = "titus.worker.resubmission.interval.secs")
    @DefaultValue("5:10:20")
    String getWorkerResubmitIntervalSecs();

    @PropertyName(name = "titus.worker.expire.resubmit.delay.secs")
    @DefaultValue("300")
    long getExpireWorkerResubmitDelaySecs();

    @PropertyName(name = "titus.worker.expire.resubmit.execution.interval.secs")
    @DefaultValue("120")
    long getExpireResubmitDelayExecutionIntervalSecs();

    @PropertyName(name = "titus.master.terminated.job.to.delete.delay.hours")
    @DefaultValue("360")
        // 15 days * 24 hours
    long getTerminatedJobToDeleteDelayHours();

    @PropertyName(name = "mesos.slave.attribute.zone.name")
    @DefaultValue("zone")
    String getHostZoneAttributeName();

    @PropertyName(name = "titus.agent.cluster.autoscale.by.attribute.name")
    @DefaultValue("asg")
    String getAutoscaleByAttributeName();

    @PropertyName(name = "titus.agent.cluster.autoscaler.map.hostname.attribute.name")
    @DefaultValue("id")
    String getAutoScalerMapHostnameAttributeName();

    @PropertyName(name = "titus.agent.cluster.instance.type")
    @DefaultValue("itype")
    String getInstanceTypeAttributeName();

    @PropertyName(name = "titus.master.store.worker.writes.batch.size")
    @DefaultValue("100")
    int getWorkerWriteBatchSize();

    @PropertyName(name = "titus.master.check.unique.job.id.sequence")
    @DefaultValue("true")
    boolean getCheckUniqueJobIdSequence();

    @PropertyName(name = "titus.jobspec.cpu.max")
    @DefaultValue("32")
        //r3.8xl limit
    int getMaxCPUs();

    @PropertyName(name = "titus.jobspec.memory.mb.max")
    @DefaultValue("244000")
        // r3.8xl limit
    int getMaxMemory();

    @PropertyName(name = "titus.jobspec.disk.mb.max")
    @DefaultValue("640000")
        // r3.8xl limit
    int getMaxDisk();

    @PropertyName(name = "titus.jobspec.network.mbps.max")
    @DefaultValue("6000")
        // r3.8xl limit
    int getMaxNetworkMbps();

    @PropertyName(name = "titus.jobspec.batch.instances.max")
    @DefaultValue("1000")
    int getMaxBatchInstances();

    @PropertyName(name = "titus.jobspec.service.instances.max")
    @DefaultValue("2500")
    int getMaxServiceInstances();

    @PropertyName(name = "titus.store.terminatedJobCleanUpBatchSize")
    @DefaultValue("1000")
    int getTerminatedJobCleanUpBatchSize();

    @PropertyName(name = "titusMaster.store.maxInvalidJobs")
    @DefaultValue("0")
    long getMaxInvalidJobs();
}