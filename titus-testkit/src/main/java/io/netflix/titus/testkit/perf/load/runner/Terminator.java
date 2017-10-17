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

package io.netflix.titus.testkit.perf.load.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobState;
import io.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Removes running jobs belonging to past sessions.
 */
@Singleton
public class Terminator {

    private static final Logger logger = LoggerFactory.getLogger(Terminator.class);

    private final ExecutionContext context;

    @Inject
    public Terminator(ExecutionContext context) {
        this.context = context;
    }

    public void doClean() {
        List<TitusJobInfo> allJobs = context.getClient().findAllJobs().toList().toBlocking().first();

        List<String> toRemove = new ArrayList<>();
        allJobs.forEach(j -> {
            if (isActivePreviousSessionJob(j)) {
                toRemove.add(j.getId());
            }
        });

        if (!toRemove.isEmpty()) {
            logger.warn("Removing old jobs: {}", toRemove);

            List<Observable<Void>> killActions = toRemove.stream()
                    .map(jid -> context.getClient().killJob(jid))
                    .collect(Collectors.toList());
            Observable.merge(killActions, 10).toBlocking().firstOrDefault(null);
        }
    }

    private boolean isActivePreviousSessionJob(TitusJobInfo j) {
        return TitusJobState.isActive(j.getState())
                && j.getLabels() != null
                && j.getLabels().containsKey(ExecutionContext.LABEL_SESSION);
    }
}
