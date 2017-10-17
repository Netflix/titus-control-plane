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

package io.netflix.titus.api.jobmanager.model.job;

/**
 */
public enum JobState {

    /**
     * A job is persisted in Titus, and ready to be scheduled or running.
     */
    Accepted,

    /**
     * A job still has running tasks, that were requested to terminate. No more tasks for this job are deployed.
     * Job policy update operations are not allowed.
     */
    KillInitiated,

    /**
     * A job has no running tasks, and no more tasks for it are created. Job policy update operations are not allowed.
     */
    Finished
}
