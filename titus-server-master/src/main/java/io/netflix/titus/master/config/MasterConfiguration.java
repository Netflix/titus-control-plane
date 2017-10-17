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

    @PropertyName(name = "mantis.master.apiProxyPort")
    @DefaultValue("7001")
    int getApiProxyPort();

    @PropertyName(name = "mantis.master.api.status.path")
    @DefaultValue("/api/v2/jobs/heartbeat")
    String getApiStatusUri();

    @PropertyName(name = "mantis.master.host")
    @DefaultValue("")
    String getMasterHost();

    @PropertyName(name = "mantis.master.ip")
    @DefaultValue("")
    String getMasterIP();

    @PropertyName(name = "titus.master.job.use.resource.network.mbps")
    @DefaultValue("true")
    boolean getUseNetworkMbpsAttribute();

    @PropertyName(name = "titus.master.job.security.groups.assignment.disable")
    @DefaultValue("false")
    boolean getDisableSecurityGroupsAssignments();

    @PropertyName(name = "titus.master.job.security.groups.default.list")
    @DefaultValue("nf.infrastructure,nf.datacenter")
    String getDefaultSecurityGroupsList();

    @PropertyName(name = "mesos.master.location")
    @DefaultValue("localhost:5050")
    String getMasterLocation();

    @PropertyName(name = "mesos.titan.executor")
    @DefaultValue("/apps/titus-executor/bin/run")
    String pathToTitusExecutor();

    @PropertyName(name = "mantis.master.active.slave.attribute.name")
    @DefaultValue("SLAVE_ID")
    String getActiveSlaveAttributeName();

    @PropertyName(name = "mantis.master.framework.name")
    @DefaultValue("TitusFramework")
    String getMesosFrameworkName();

    @PropertyName(name = "mantis.master.mesos.failover.timeout.secs")
    @DefaultValue("604800.0")
        // 604800 secs = 1 week
    double getMesosFailoverTimeOutSecs();

    @PropertyName(name = "mesos.task.reconciliation.interval.secs")
    @DefaultValue("300")
    long getMesosTaskReconciliationIntervalSecs();

    @PropertyName(name = "mesos.lease.offer.expiry.secs")
    @DefaultValue("300")
    long getMesosLeaseOfferExpirySecs();

    @PropertyName(name = "mantis.jobs.max.jars.per.named.job")
    @DefaultValue("10")
    int getMaximumNumberOfJarsPerJobName();

    @PropertyName(name = "mantis.named.job.store.completed.jobs")
    @DefaultValue("false")
    boolean getStoreCompletedJobsForNamedJob();

    @PropertyName(name = "mantis.worker.resubmission.interval.secs")
    @DefaultValue("5:10:20")
    String getWorkerResubmitIntervalSecs();

    @PropertyName(name = "mantis.worker.expire.resubmit.delay.secs")
    @DefaultValue("300")
    long getExpireWorkerResubmitDelaySecs();

    @PropertyName(name = "mantis.worker.expire.resubmit.execution.interval.secs")
    @DefaultValue("120")
    long getExpireResubmitDelayExecutionIntervalSecs();

    @PropertyName(name = "mantis.master.terminated.job.to.delete.delay.hours")
    @DefaultValue("360")
        // 15 days * 24 hours
    long getTerminatedJobToDeleteDelayHours();

    @PropertyName(name = "mesos.slave.attribute.zone.name")
    @DefaultValue("AWSZone")
    String getHostZoneAttributeName();

    @PropertyName(name = "mantis.agent.cluster.autoscale.by.attribute.name")
    @DefaultValue("CLUSTER_NAME")
    String getAutoscaleByAttributeName();

    @PropertyName(name = "mantis.agent.cluster.autoscaler.map.hostname.attribute.name")
    @DefaultValue("EC2_INSTANCE_ID")
    String getAutoScalerMapHostnameAttributeName();

    @PropertyName(name = "mantis.agent.cluster.instance.type")
    @DefaultValue("itype")
    String getInstanceTypeAttributeName();

    @PropertyName(name = "mantis.agent.cluster.pinning.enabled")
    @DefaultValue("true")
    boolean getMultiAgentClusterPinningEnabled();

    @PropertyName(name = "mantis.master.store.worker.writes.batch.size")
    @DefaultValue("100")
    int getWorkerWriteBatchSize();

    @PropertyName(name = "mantis.master.check.unique.job.id.sequence")
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
    @DefaultValue("10000")
    int getMaxBatchInstances();

    @PropertyName(name = "titus.jobspec.service.instances.max")
    @DefaultValue("1000")
    int getMaxServiceInstances();

    @PropertyName(name = "titus.jobspec.runtimelimit.secs.default")
    @DefaultValue("432000")
    long getDefaultRuntimeLimit();

    @PropertyName(name = "titus.jobspec.runtimelimit.secs.max")
    @DefaultValue("864000")
    long getMaxRuntimeLimit();

    @PropertyName(name = "titus.store.terminatedJobCleanUpBatchSize")
    @DefaultValue("1000")
    int getTerminatedJobCleanUpBatchSize();

    @PropertyName(name = "titusMaster.store.maxInvalidJobs")
    @DefaultValue("0")
    long getMaxInvalidJobs();
}
