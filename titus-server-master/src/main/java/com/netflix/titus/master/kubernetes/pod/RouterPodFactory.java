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

package com.netflix.titus.master.kubernetes.pod;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.StringExt;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class RouterPodFactory implements PodFactory {
    private static final Logger logger = LoggerFactory.getLogger(RouterPodFactory.class);

    private final KubePodConfiguration configuration;
    private final Map<String, PodFactory> versionedPodFactories;

    private final Supplier<String> podSpecVersionRoutingRules;
    private final Function<String, Map<String, Pattern>> patternsByVersion;

    @Inject
    public RouterPodFactory(KubePodConfiguration configuration,
                            Map<String, PodFactory> versionedPodFactories) {
        this.configuration = configuration;
        this.versionedPodFactories = versionedPodFactories;

        podSpecVersionRoutingRules = configuration::getPodSpecVersionRoutingRules;

        patternsByVersion = Evaluators.memoizeLast((spec, lastCompiledPatterns) -> {
            logger.info("Detected new routing rules, compiling them: {}", spec);
            try {
                return extractPatterns(podSpecVersionRoutingRules.get());
            } catch (RuntimeException e) {
                logger.error("Bad cell routing spec, ignoring: {}", spec);
                return lastCompiledPatterns.orElseThrow(() -> e /* there is nothing to do if the first spec is bad */);
            }
        });

        patternsByVersion.apply(podSpecVersionRoutingRules.get());
    }

    /**
     * Create an allow list per version and check to see if app name or capacity group is in any allow list. If it is, then
     * route to the first occurrence of the version. If the app name or capacity group is not in any allow list then
     * route to the default version. e.g. v1=(app1,app2,cg1,cg3);v2=(app4)
     */
    @Override
    public V1Pod buildV1Pod(Job<?> job, Task task, boolean useKubeScheduler, boolean useKubePv) {
        Map<String, Pattern> versionPatterns = patternsByVersion.apply(podSpecVersionRoutingRules.get());

        for (Map.Entry<String, Pattern> versionPattern : versionPatterns.entrySet()) {
            String version = versionPattern.getKey();
            Pattern pattern = versionPattern.getValue();

            String applicationName = job.getJobDescriptor().getApplicationName();
            String capacityGroup = job.getJobDescriptor().getCapacityGroup();
            if (pattern.matcher(applicationName).matches() || pattern.matcher(capacityGroup).matches()) {
                return buildV1PodByVersion(version, job, task, useKubeScheduler, useKubePv);
            }
        }
        return buildV1PodByVersion(configuration.getDefaultPodSpecVersion(), job, task, useKubeScheduler, useKubePv);
    }

    private V1Pod buildV1PodByVersion(String version, Job<?> job, Task task, boolean useKubeScheduler, boolean useKubePv) {
        if (versionedPodFactories.containsKey(version)) {
            return versionedPodFactories.get(version).buildV1Pod(job, task, useKubeScheduler, useKubePv);
        } else {
            throw new RuntimeException("Unable to create pod spec with invalid version: " + version);
        }
    }

    @VisibleForTesting
    static Map<String, Pattern> extractPatterns(String patterns) {
        Map<String, Pattern> extractedPatterns = new HashMap<>();
        String[] versionStatements = patterns.split(";");
        for (String versionStatement : versionStatements) {
            String[] parts = versionStatement.split("=");
            String version = parts[0];
            String regex = parts[1];
            if (StringExt.isNotEmpty(version) && StringExt.isNotEmpty(regex)) {
                extractedPatterns.put(version, Pattern.compile(regex));
            }
        }
        return extractedPatterns;
    }
}
