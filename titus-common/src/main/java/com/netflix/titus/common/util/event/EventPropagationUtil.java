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

package com.netflix.titus.common.util.event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;

/**
 * Event propagation tracking over many channels. Sever side and client side timestamps are recorded in pairs.
 * After snapshot example for TJC event with updateTimestamp=1 send to a client connected to Federation endpoint: <br/>
 * <code>
 * event.propagation.stages: before=10,after=30;before=50,after=70;before=100,after=120</li>
 * </code>
 * Latencies:
 * <ul>
 *     <li>TJC processing latency = 10 - 1 = 9ms</li>
 *     <li>TJC -> Gateway latency = 30 - 10 = 20ms</li>
 *     <li>Gateway processing latency = 50 - 30 = 20ms</li>
 *     <li>Gateway -> Federation latency = 70 - 50 = 20ms</li>
 *     <li>Federation processing latency = 100 - 70 = 30ms</li>
 *     <li>Federation -> client latency = 100 - 120 = 20ms</li>
 * </ul>
 * Total event propagation latency = 119ms
 * <p>
 * For events generated before the snapshot marker, we cannot use lastUpdateTimestamp as a starting point. These
 * events will miss the TJC processing latency.
 */
public class EventPropagationUtil {

    public static final String EVENT_ATTRIBUTE_PROPAGATION_STAGES = "event.propagation.stages";
    public static final int MAX_LENGTH_EVENT_PROPAGATION_STAGE = 500;

    public static String buildNextStage(String id, Map<String, String> attributes, long beforeTimestamp, long afterTimestamp) {
        String stages = attributes.get(EVENT_ATTRIBUTE_PROPAGATION_STAGES);
        long delaysMs = afterTimestamp - beforeTimestamp;
        String nextStagePayload = id + "(" + delaysMs + "ms):" + "before=" + beforeTimestamp + ",after=" + afterTimestamp;
        if (stages != null && stages.length() < MAX_LENGTH_EVENT_PROPAGATION_STAGE) {
            return stages + ";" + nextStagePayload;
        }
        return nextStagePayload;
    }

    public static Map<String, String> copyAndAddNextStage(String id, Map<String, String> attributes, long beforeTimestamp, long afterTimestamp) {
        String nextStages = buildNextStage(id, attributes, beforeTimestamp, afterTimestamp);
        return CollectionsExt.copyAndAdd(attributes, EVENT_ATTRIBUTE_PROPAGATION_STAGES, nextStages);
    }

    /**
     * Parse the event propagation data stored in {@link #EVENT_ATTRIBUTE_PROPAGATION_STAGES}.
     * The delay estimation is based on timestamps. As clocks may not be perfectly synchronized between machines, it
     * is only estimate.
     */
    public static Optional<EventPropagationTrace> parseTrace(Map<String, String> attributes,
                                                             boolean snapshot,
                                                             long lastUpdateTimestamp,
                                                             List<String> labels) {
        String stages = attributes.get(EVENT_ATTRIBUTE_PROPAGATION_STAGES);
        if (StringExt.isEmpty(stages)) {
            return Optional.empty();
        }

        Map<String, Long> stagesMap = new HashMap<>();
        int labelPos = 0;
        long previousTimestamp = lastUpdateTimestamp;
        try {
            String[] stagesList = stages.split(";");
            for (String beforeAfter : stagesList) {
                if (labelPos >= labels.size()) {
                    break;
                }
                String[] pair = beforeAfter.split(",");
                if (pair.length != 2) {
                    return Optional.empty();
                }
                String[] before = pair[0].split("=");
                if (before.length != 2) {
                    return Optional.empty();
                }
                long beforeTimestamp = Long.parseLong(before[1]);
                String[] after = pair[1].split("=");
                if (after.length != 2) {
                    return Optional.empty();
                }
                long afterTimestamp = Long.parseLong(after[1]);

                long internalDelay;
                if (labelPos == 0) {
                    if (snapshot) {
                        // Snapshot replays pre-existing data so we cannot use lastUpdateTimestamp to asses propagation latency.
                        internalDelay = 0;
                    } else {
                        internalDelay = lastUpdateTimestamp >= 0 ? beforeTimestamp - lastUpdateTimestamp : 0;
                    }
                } else {
                    internalDelay = beforeTimestamp - previousTimestamp;
                }
                // If clocks are not the same, we can get negative delay. The best we can do is to make it is not less than zero.
                stagesMap.put(labels.get(labelPos++), Math.max(0, internalDelay));
                if (labelPos >= labels.size()) {
                    break;
                }
                // If clocks are not the same, we can get negative delay. The best we can do is to make it is not less than zero.
                stagesMap.put(labels.get(labelPos++), Math.max(0, afterTimestamp - beforeTimestamp));
                previousTimestamp = afterTimestamp;
            }
        } catch (Exception e) { // Catch anything to prevent error propagation
            return Optional.empty();
        }
        return Optional.of(new EventPropagationTrace(snapshot, stagesMap));
    }
}
