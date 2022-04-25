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

package com.netflix.titus.master.kubernetes;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.master.kubernetes.client.model.PodWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class DefaultContainerResultCodeResolver implements ContainerResultCodeResolver {

    private static final Logger logger = LoggerFactory.getLogger(DefaultContainerResultCodeResolver.class);

    private final Function<String, Matcher> invalidRequestMessageMatcherFactory;
    private final Function<String, Matcher> crashedMessageMatcherFactory;
    private final Function<String, Matcher> transientSystemErrorMessageMatcherFactory;
    private final Function<String, Matcher> localSystemErrorMessageMatcherFactory;
    private final Function<String, Matcher> unknownSystemErrorMessageMatcherFactory;

    @Inject
    public DefaultContainerResultCodeResolver(KubernetesConfiguration kubernetesConfiguration) {
        this.invalidRequestMessageMatcherFactory = RegExpExt.dynamicMatcher(kubernetesConfiguration::getInvalidRequestMessagePattern, "invalidRequestMessagePattern", Pattern.DOTALL, logger);
        this.crashedMessageMatcherFactory = RegExpExt.dynamicMatcher(kubernetesConfiguration::getCrashedMessagePattern, "crashedMessagePattern", Pattern.DOTALL, logger);
        this.transientSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(kubernetesConfiguration::getTransientSystemErrorMessagePattern, "transientSystemErrorMessagePattern", Pattern.DOTALL, logger);
        this.localSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(kubernetesConfiguration::getLocalSystemErrorMessagePattern, "localSystemErrorMessagePattern", Pattern.DOTALL, logger);
        this.unknownSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(kubernetesConfiguration::getUnknownSystemErrorMessagePattern, "unknownSystemErrorMessagePattern", Pattern.DOTALL, logger);
    }

    @Override
    public Optional<String> resolve(TaskState nextTaskState, Task task, PodWrapper podWrapper) {
        if (nextTaskState != TaskState.Finished) {
            return Optional.empty();
        }
        String reasonMessage = podWrapper.getMessage();

        // If a pod crashed, this could be during the container setup (for example disk full) or while the container
        // is running. The former case we should classify as a transient system error, the latter a user error.
        if ("TASK_CRASHED".equals(podWrapper.getReason())) {
            // System error
            if (TaskState.isBefore(task.getStatus().getState(), TaskState.Started)) {
                return Optional.of(processReasonMessage(reasonMessage).orElse(TaskStatus.REASON_LOCAL_SYSTEM_ERROR));
            }
            // User error
            return Optional.of(processReasonMessage(reasonMessage).orElse(TaskStatus.REASON_FAILED));
        }

        return processReasonMessage(reasonMessage);
    }

    private Optional<String> processReasonMessage(String reasonMessage) {
        if (StringExt.isEmpty(reasonMessage)) {
            return Optional.empty();
        }
        if (invalidRequestMessageMatcherFactory.apply(reasonMessage).matches()) {
            return Optional.of(TaskStatus.REASON_INVALID_REQUEST);
        }
        if (crashedMessageMatcherFactory.apply(reasonMessage).matches()) {
            return Optional.of(TaskStatus.REASON_CRASHED);
        }
        if (transientSystemErrorMessageMatcherFactory.apply(reasonMessage).matches()) {
            return Optional.of(TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR);
        }
        if (localSystemErrorMessageMatcherFactory.apply(reasonMessage).matches()) {
            return Optional.of(TaskStatus.REASON_LOCAL_SYSTEM_ERROR);
        }
        if (unknownSystemErrorMessageMatcherFactory.apply(reasonMessage).matches()) {
            return Optional.of(TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR);
        }
        return Optional.empty();
    }
}
