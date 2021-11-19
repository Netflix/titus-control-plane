/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.topology;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.JobConstraints;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetPolicy;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1LabelSelector;
import io.kubernetes.client.openapi.models.V1TopologySpreadConstraint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.getJobDesiredSize;
import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.isServiceJob;

@Singleton
public class DefaultTopologyFactory implements TopologyFactory {
    private static final Logger logger = LoggerFactory.getLogger(DefaultTopologyFactory.class);


    private final KubePodConfiguration configuration;
    private final FeatureActivationConfiguration features;
    private final Function<String, Matcher> jobsWithNoSpreadingMatcher;

    @Inject
    public DefaultTopologyFactory(KubePodConfiguration configuration, FeatureActivationConfiguration features) {
        this.configuration = configuration;
        this.features = features;

        this.jobsWithNoSpreadingMatcher = RegExpExt.dynamicMatcher(configuration::getDisabledJobSpreadingPattern,
                "disabledJobSpreadingPattern", Pattern.DOTALL, logger);
    }

    @Override
    @VisibleForTesting
    public List<V1TopologySpreadConstraint> buildTopologySpreadConstraints(Job<?> job) {
        List<V1TopologySpreadConstraint> constraints = new ArrayList<>();
        buildZoneTopologySpreadConstraints(job).ifPresent(constraints::add);
        buildJobTopologySpreadConstraints(job).ifPresent(constraints::add);
        return constraints;
    }

    private Optional<V1TopologySpreadConstraint> buildZoneTopologySpreadConstraints(Job<?> job) {
        boolean zoneHard = Boolean.parseBoolean(JobFunctions.findHardConstraint(job, JobConstraints.ZONE_BALANCE).orElse("false"));
        boolean zoneSoft = Boolean.parseBoolean(JobFunctions.findSoftConstraint(job, JobConstraints.ZONE_BALANCE).orElse("false"));
        if (!zoneHard && !zoneSoft) {
            return Optional.empty();
        }

        V1TopologySpreadConstraint zoneConstraint = new V1TopologySpreadConstraint()
                .topologyKey(KubeConstants.NODE_LABEL_ZONE)
                .labelSelector(new V1LabelSelector().matchLabels(Collections.singletonMap(KubeConstants.POD_LABEL_JOB_ID, job.getId())))
                .maxSkew(1);

        if (zoneHard) {
            zoneConstraint.whenUnsatisfiable("DoNotSchedule");
        } else {
            zoneConstraint.whenUnsatisfiable("ScheduleAnyway");
        }
        return Optional.of(zoneConstraint);
    }

    private Optional<V1TopologySpreadConstraint> buildJobTopologySpreadConstraints(Job<?> job) {
        if (!isJobSpreadingEnabled(job)) {
            return Optional.empty();
        }
        int maxSkew = getJobMaxSkew(job);
        if (maxSkew <= 0) {
            return Optional.empty();
        }

        V1TopologySpreadConstraint nodeConstraint = new V1TopologySpreadConstraint()
                .topologyKey(KubeConstants.NODE_LABEL_MACHINE_ID)
                .labelSelector(new V1LabelSelector().matchLabels(Collections.singletonMap(KubeConstants.POD_LABEL_JOB_ID, job.getId())))
                .maxSkew(maxSkew)
                .whenUnsatisfiable("ScheduleAnyway");
        return Optional.of(nodeConstraint);
    }

    /**
     * Spreading is by default enabled for service jobs and disabled for batch jobs or jobs binpacked for relocation.
     */
    private boolean isJobSpreadingEnabled(Job<?> job) {
        if (jobsWithNoSpreadingMatcher.apply(job.getJobDescriptor().getApplicationName()).matches()) {
            return false;
        }
        if (jobsWithNoSpreadingMatcher.apply(job.getJobDescriptor().getCapacityGroup()).matches()) {
            return false;
        }
        String spreadingEnabledAttr = job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_SPREADING_ENABLED);
        if (spreadingEnabledAttr != null) {
            return Boolean.parseBoolean(spreadingEnabledAttr);
        }
        if (features.isRelocationBinpackingEnabled() && JobManagerUtil.getRelocationBinpackMode(job).isPresent()) {
            // by default do not spread jobs that are marked for binpacking due to relocation
            return false;
        }
        return isServiceJob(job);
    }

    /**
     * Get max skew from a job descriptor or compute a value based on the job type and its configured disruption budget.
     *
     * @return -1 if max skew not set or is invalid
     */
    private int getJobMaxSkew(Job<?> job) {
        String maxSkewAttr = job.getJobDescriptor().getAttributes().get(JobAttributes.JOB_ATTRIBUTES_SPREADING_MAX_SKEW);
        if (maxSkewAttr != null) {
            try {
                int maxSkew = Integer.parseInt(maxSkewAttr);
                if (maxSkew > 0) {
                    return maxSkew;
                }
            } catch (Exception ignore) {
            }
        }

        DisruptionBudgetPolicy policy = job.getJobDescriptor().getDisruptionBudget().getDisruptionBudgetPolicy();
        // Job spreading is only relevant for jobs that care about the availability.
        if (!(policy instanceof AvailabilityPercentageLimitDisruptionBudgetPolicy)) {
            return -1;
        }
        int jobSize = getJobDesiredSize(job);
        if (jobSize <= 1) {
            return 1;
        }
        double alpha = configuration.getJobSpreadingSkewAlpha();
        if (alpha <= 0) {
            return -1;
        }
        int skew = (int) (Math.floor(jobSize / alpha));
        // The skew must be between 1 and the configured max skew.
        return Math.max(1, Math.min(skew, configuration.getJobSpreadingMaxSkew()));
    }
}
