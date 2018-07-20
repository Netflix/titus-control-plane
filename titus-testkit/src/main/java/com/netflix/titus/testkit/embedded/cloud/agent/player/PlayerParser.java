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

package com.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.netflix.titus.common.util.NumberSequence;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.common.util.unit.TimeUnitExt;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus.SimulatedTaskState;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container player scenario parser. The scenario is encoded in the environment variables like in the examples below:
 * <p>
 * {@code TASK_LIFECYCLE_1=selector: slots=0.. slotStep=2; launched: delay=2s; startInitiated: delay=3s; started: delay=60s; killInitiated: delay=5s}<br>
 * {@code TASK_LIFECYCLE_2=selector: slots=1.. slotStep=2; launched: delay=2s; startInitiated: action=finish titusReasonCode=failed failureMessage='rate limited'}<br>
 * <p>or:<br/>
 * {@code TASK_LIFECYCLE_1=selector: resubmits=0,1 slots=0.. slotStep=2; launched: delay=2s; startInitiated: action=forget}<br>
 * {@code TASK_LIFECYCLE_2=selector: resubmits=2.. instances=flex1,flex2; launched: delay=2s; startInitiated: delay=3s; started: delay=60s; killInitiated: delay=5s}<br>
 */
class PlayerParser {

    private static final Logger logger = LoggerFactory.getLogger(PlayerParser.class);

    private static final Pattern PART_RE = Pattern.compile("(selector|launched|startInitiated|started|killInitiated):\\s*(.*)");
    private static final Pattern PARAMETER_RE = Pattern.compile("([^=]+)=(.*)");

    static final String KEY_PREFIX = "TASK_LIFECYCLE_";

    static List<Pair<RuleSelector, ContainerRules>> parse(Map<String, String> env) {
        try {
            return parseInternal(env).onErrorGet(PlayerParser::crash);
        } catch (Exception e) {
            logger.info("Unexpected error during task lifecycle data parsing", e);
            return crash(String.format("Unexpected error during task lifecycle data parsing (%s)", e.getMessage()));
        }
    }

    static Either<List<Pair<RuleSelector, ContainerRules>>, String> parseInternal(Map<String, String> env) {
        SortedMap<Integer, Pair<RuleSelector, ContainerRules>> result = new TreeMap<>(Integer::compareTo);

        for (Map.Entry<String, String> entry : env.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (!key.startsWith(KEY_PREFIX)) {
                continue;
            }

            Optional<Integer> index = findTaskLifecycleIndex(key);
            if (!index.isPresent()) {
                return Either.ofError(String.format("Invalid index %s in task lifecycle entry: %s=%s", key, key, value));
            }
            Either<Pair<RuleSelector, ContainerRules>, String> parsedValue = parseLine(index.get(), value);
            if (parsedValue.hasError()) {
                return Either.ofError(String.format("%s in task lifecycle entry: %s=%s", parsedValue.getError(), key, value));
            }
            result.put(index.get(), parsedValue.getValue());
        }
        return Either.ofValue(new ArrayList<>(result.values()));
    }

    private static Optional<Integer> findTaskLifecycleIndex(String key) {
        if (key.startsWith(KEY_PREFIX)) {
            try {
                return Optional.of(Integer.parseInt(key.substring(KEY_PREFIX.length())));
            } catch (NumberFormatException e) {
            }
        }
        return Optional.empty();
    }

    private static Either<Pair<RuleSelector, ContainerRules>, String> parseLine(int index, String value) {
        RuleSelector selector = null;
        Map<SimulatedTaskState, ContainerStateRule> stateRules = new HashMap<>();

        for (String part : value.split("\\s*;\\s*")) {
            Matcher partMatcher = PART_RE.matcher(part);
            if (!partMatcher.matches()) {
                return Either.ofError(String.format("Syntax error during parsing fragment '%s'", part));
            }
            String partName = partMatcher.group(1);
            Map<String, String> parameters = new HashMap<>();
            for (String parameter : partMatcher.group(2).split("\\s+")) {
                Matcher matcher = PARAMETER_RE.matcher(parameter);
                if (!matcher.matches()) {
                    return Either.ofError(String.format("Invalid parameter '%s'", parameter));
                }
                parameters.put(matcher.group(1).trim(), matcher.group(2).trim());
            }
            if (partName.equals("selector")) {
                Either<RuleSelector, String> selectorResult = parseSelectorEntry(parameters);
                if (selectorResult.hasError()) {
                    return Either.ofError(selectorResult.getError());
                }
                selector = selectorResult.getValue();
            } else {
                Either<ContainerStateRule, String> stateResult = parseStateRule(parameters);
                if (stateResult.hasError()) {
                    return Either.ofError(stateResult.getError());
                }
                stateRules.put(toTaskStateName(partName), stateResult.getValue());
            }
        }

        if (selector == null) {
            selector = RuleSelector.everything();
        }
        if (stateRules.isEmpty()) {
            return Either.ofError(String.format("Task lifecycle with incomplete state rules: %s=%s", index, value));
        }

        return Either.ofValue(Pair.of(selector, new ContainerRules(stateRules)));
    }

    private static SimulatedTaskState toTaskStateName(String partName) {
        switch (partName) {
            case "launched":
                return SimulatedTaskState.Launched;
            case "startInitiated":
                return SimulatedTaskState.StartInitiated;
            case "started":
                return SimulatedTaskState.Started;
            case "killInitiated":
                return SimulatedTaskState.KillInitiated;
        }
        throw new IllegalStateException("Unknown task state: " + partName);
    }

    private static Either<RuleSelector, String> parseSelectorEntry(Map<String, String> parameters) {
        RuleSelector.Builder builder = RuleSelector.newBuilder();

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            switch (name) {
                case "slots":
                    Either<NumberSequence, String> parsedSlots = NumberSequence.parse(value);
                    if (parsedSlots.hasError()) {
                        return Either.ofError(String.format("Invalid container slot sequence '%s'", parsedSlots.getError()));
                    }
                    builder.withSlots(parsedSlots.getValue());
                    break;
                case "slotStep":
                    try {
                        builder.withSlotStep(Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        return Either.ofError(String.format("Invalid index step '%s'", value));
                    }
                    break;
                case "resubmits":
                    Either<NumberSequence, String> parsedResubmits = NumberSequence.parse(value);
                    if (parsedResubmits.hasError()) {
                        return Either.ofError(String.format("Invalid resubmit sequence '%s'", parsedResubmits.getError()));
                    }
                    builder.withResubmits(parsedResubmits.getValue());
                    break;
                case "resubmitStep":
                    try {
                        builder.withResubmitStep(Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        return Either.ofError(String.format("Invalid resubmit step '%s'", value));
                    }
                    break;
                case "instances":
                    builder.withInstances(value.split("\\s*,\\s*"));
                    break;
                default:
                    return Either.ofError(String.format("Invalid parameter name '%s'", name));
            }
        }

        return Either.ofValue(builder.build());
    }

    private static Either<ContainerStateRule, String> parseStateRule(Map<String, String> parameters) {
        ContainerStateRule.Builder builder = ContainerStateRule.newBuilder();

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            switch (name) {
                case "delay":
                    Optional<Long> delayMs = TimeUnitExt.toMillis(value);
                    if (!delayMs.isPresent()) {
                        return Either.ofError(String.format("Invalid delay value '%s'", value));
                    }
                    builder.withDelayInStateMs(delayMs.get());
                    break;
                case "action":
                    builder.withAction(StringExt.parseEnumIgnoreCase(value, ContainerStateRule.Action.class));
                    break;
                case "mesosTerminalState":
                    builder.withMesosTerminalState(StringExt.parseEnumIgnoreCase(value, Protos.TaskState.class));
                    break;
                case "mesosReasonCode":
                    builder.withMesosReasonCode(StringExt.parseEnumIgnoreCase(value, Protos.TaskStatus.Reason.class));
                    break;
                case "titusReasonCode":
                    builder.withTitusReasonCode(value);
                    break;
                case "reasonMessage":
                    builder.withReasonMessage(value);
                    break;
                default:
                    return Either.ofError(String.format("Invalid parameter name '%s'", name));
            }
        }

        return Either.ofValue(builder.build());
    }

    private static List<Pair<RuleSelector, ContainerRules>> crash(String reasonMessage) {
        return Collections.singletonList(
                Pair.of(
                        RuleSelector.everything(),
                        new ContainerRules(Collections.singletonMap(
                                SimulatedTaskState.Launched,
                                ContainerStateRule.newBuilder()
                                        .withMesosTerminalState(Protos.TaskState.TASK_ERROR)
                                        .withMesosReasonCode(Protos.TaskStatus.Reason.REASON_CONTAINER_LAUNCH_FAILED)
                                        .withReasonMessage(reasonMessage)
                                        .build()
                        ))
                )
        );
    }
}
