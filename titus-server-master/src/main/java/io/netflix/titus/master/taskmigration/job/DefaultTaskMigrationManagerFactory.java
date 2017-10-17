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

package io.netflix.titus.master.taskmigration.job;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.Lists;
import io.netflix.titus.api.model.MigrationPolicy;
import io.netflix.titus.api.model.SelfManagedMigrationPolicy;
import io.netflix.titus.common.util.limiter.Limiters;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import io.netflix.titus.master.taskmigration.TaskMigrationDetails;
import io.netflix.titus.master.taskmigration.TaskMigrationManager;
import io.netflix.titus.master.taskmigration.TaskMigrationManagerFactory;
import io.netflix.titus.master.taskmigration.V2TaskMigrationDetails;

@Singleton
public class DefaultTaskMigrationManagerFactory implements TaskMigrationManagerFactory {
    private static final int PER_SECOND_INTERVAL = 1;

    private final ServiceJobTaskMigratorConfig config;
    private final TokenBucket terminateTokenBucket;

    @Inject
    public DefaultTaskMigrationManagerFactory(ServiceJobTaskMigratorConfig config) {
        this.config = config;
        this.terminateTokenBucket = Limiters.createFixedIntervalTokenBucket("DefaultMigrationTerminates", config.getTerminateTokenBucketCapacity(), 0,
                config.getTerminateTokenBucketRefillRatePerSecond(), PER_SECOND_INTERVAL, TimeUnit.SECONDS);
    }

    @Override
    public TaskMigrationManager newTaskMigrationManager(TaskMigrationDetails taskMigrationDetails) {
        if (taskMigrationDetails instanceof V2TaskMigrationDetails) {
            V2TaskMigrationDetails v2TaskMigrationDetails = (V2TaskMigrationDetails) taskMigrationDetails;
            MigrationPolicy migrationPolicy = v2TaskMigrationDetails.getMigrationPolicy();
            if (migrationPolicy instanceof SelfManagedMigrationPolicy) {
                long migrationDeadline = v2TaskMigrationDetails.getMigrationDeadline();
                long timeout = config.getSelfManagedTimeoutMs();
                if (migrationDeadline > 0) {
                    timeout = Math.max(0, migrationDeadline - System.currentTimeMillis());
                }

                List<TaskMigrationManager> migrationManagers = Lists.newArrayList(
                        new DelayTaskMigrationManager(timeout),
                        new DefaultTaskMigrationManager(config, terminateTokenBucket)
                );
                return new CompositeTaskMigrationManager(migrationManagers);
            }
        }
        return new DefaultTaskMigrationManager(config, terminateTokenBucket);
    }
}
