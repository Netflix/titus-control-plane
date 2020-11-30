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

package com.netflix.titus.master.config;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;

public interface MasterConfiguration {

    @PropertyName(name = "titus.localmode")
    @DefaultValue("false")
    boolean isLocalMode();

    @PropertyName(name = "region")
    String getRegion();

    @PropertyName(name = "titus.master.cellName")
    @DefaultValue("dev")
    String getCellName();

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

    @PropertyName(name = "mesos.lease.offer.max.reject.count")
    @DefaultValue("4")
    int getMesosLeaseMaxRejectCount();

    @PropertyName(name = "mesos.slave.attribute.zone.name")
    @DefaultValue("zone")
    String getHostZoneAttributeName();

    @PropertyName(name = "titus.jobcoordinator.pod.containerInfoEnvEnabled")
    @DefaultValue("true")
    boolean isContainerInfoEnvEnabled();
}