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

package io.netflix.titus.master.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.store.V2JobStore;
import io.netflix.titus.master.store.V2StageMetadataWritable;
import io.netflix.titus.master.store.V2WorkerMetadataWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;

public class JobMgrUtils {

    private static final Logger logger = LoggerFactory.getLogger(JobMgrUtils.class);

    public static List<? extends Object> getAllWorkers(V2JobMetadata mjmd,
                                                       Func1<V2WorkerMetadata, Boolean> filter,
                                                       Func1<V2WorkerMetadata, ? extends Object> mapFunc) {
        List<Object> result = new ArrayList<>();
        for (V2StageMetadata msmd : mjmd.getStageMetadata()) {
            for (V2WorkerMetadata mwmd : msmd.getAllWorkers()) {
                if (filter.call(mwmd)) {
                    result.add(mapFunc.call(mwmd));
                }
            }
        }
        return result;
    }

    public static String report(V2JobMetadata mjmd) {
        try {
            V2StageMetadata stage = mjmd.getStageMetadata(1);

            Map<String, Object> report = new HashMap<>();

            report.put("jobId", mjmd.getJobId());
            report.put("instances", stage.getNumWorkers());
            report.put("min", stage.getScalingPolicy().getMin());
            report.put("desired", stage.getScalingPolicy().getDesired());
            report.put("max", stage.getScalingPolicy().getMax());

            List<String> active = new ArrayList<>();
            List<String> finished = new ArrayList<>();
            List<String> tombstones = new ArrayList<>();
            stage.getAllWorkers().forEach(w -> {
                String taskId = WorkerNaming.getWorkerName(w.getJobId(), w.getWorkerIndex(), w.getWorkerNumber());
                if (V2JobState.isTerminalState(w.getState())) {
                    if (w.getReason() == JobCompletedReason.TombStone) {
                        tombstones.add(taskId);
                    } else {
                        finished.add(taskId);
                    }
                } else {
                    active.add(taskId);
                }
            });
            report.put("activeTasks", active);
            report.put("finished", finished);
            report.put("tombstones", tombstones);

            List<String> indexAssignments = stage.getWorkerByIndexMetadataSet().stream()
                    .map(w -> WorkerNaming.getWorkerName(w.getJobId(), w.getWorkerIndex(), w.getWorkerNumber()))
                    .collect(Collectors.toList());
            report.put("indexAssignments", indexAssignments);

            try {
                return ObjectMappers.compactMapper().writeValueAsString(report);
            } catch (JsonProcessingException e) {
                return "{}";
            }
        } catch (Exception e) {
            logger.info("Job report generation failure", e);
            return "Job report generation failure";
        }
    }

    public static List<Integer> getTombStoneSlots(V2JobMetadata mjmd) {
        List<Integer> result = getAllWorkers(
                mjmd,
                w -> w.getState() == V2JobState.Failed && w.getReason() == JobCompletedReason.TombStone,
                V2WorkerMetadata::getWorkerIndex
        ).stream().map(slot -> (Integer) slot).collect(Collectors.toList());
        Collections.sort(result);
        logger.debug("Tombstones of job {}: {}", mjmd.getJobId(), result);
        return result;
    }

    public static V2WorkerMetadataWritable getV2WorkerMetadataWritable(V2JobMetadata mjmd, int workerNumber) throws InvalidJobException {
        return (V2WorkerMetadataWritable) mjmd.getWorkerByNumber(workerNumber);
    }

    public static V2WorkerMetadata getArchivedWorker(V2JobStore store, String jobId, int workerNumber) throws IOException {
        for (V2WorkerMetadata mwmd : store.getArchivedWorkers(jobId)) {
            if (mwmd.getWorkerNumber() == workerNumber) {
                return mwmd;
            }
        }
        return null;
    }

    /**
     * As we may have tomb stoned slots in between active tasks, the effective worker number must include the internal
     * tomb stoned slots. For example job {0, 1T, 2, 3T} has two workers and 1 tombstone, so adjusted number for
     * two workers is 3 (includes the internal slot 1T).
     */
    public static int getAdjustedWorkerNum(V2StageMetadataWritable stage, int workerNum, boolean withTrailingTombstones) throws InvalidJobException {
        int workerCount = 0;
        int adjustedWorkerIdx = 0;
        int allWorkersSize = stage.getAllWorkers().size();

        while (workerCount < workerNum && adjustedWorkerIdx < allWorkersSize) {
            V2WorkerMetadata worker = stage.getWorkerByIndex(adjustedWorkerIdx);
            if (!isTombStoned(worker)) {
                workerCount++;
            }
            adjustedWorkerIdx++;
        }

        if (withTrailingTombstones) {
            while (adjustedWorkerIdx < allWorkersSize) {
                V2WorkerMetadata worker = stage.getWorkerByIndex(adjustedWorkerIdx);
                if (!isTombStoned(worker)) {
                    throw new InvalidJobException("Inconsistent job state " + stage.getJobId() + " - found too many workers");
                }
                adjustedWorkerIdx++;
            }
        }

        return adjustedWorkerIdx;
    }

    public static boolean isTombStoned(V2WorkerMetadata worker) {
        return worker.getState() == V2JobState.Failed && worker.getReason() == JobCompletedReason.TombStone;
    }
}
