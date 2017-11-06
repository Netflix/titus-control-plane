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

package io.netflix.titus.master.loadbalancer.service;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;

/**
 * Keep a thread safe collection of JobLoadBalancer associations being tracked, indexed by jobId.
 * <p>
 * This current implementation avoids locking on the read path with optimistic concurrency control and a simple and
 * inefficient implementation of CopyOnWriteHashSets.
 * </p>
 */
class Tracking {
    /**
     * Maps held in here must be all immutable (usually via Collections.unmodifiableMap).
     */
    private final ConcurrentMap<String, Set<JobLoadBalancer>> tracked = new ConcurrentHashMap<>();

    Tracking() {
    }

    /**
     * This is in the critical path and should be fast, which is why it avoids lock contention, and keeps items indexed
     * by jobId yielding O(1).
     *
     * @param jobId
     * @return a snapshot of what is currently being tracked
     */
    public Set<JobLoadBalancer> get(String jobId) {
        return tracked.getOrDefault(jobId, Collections.emptySet());
    }

    public void add(JobLoadBalancer association) {
        tracked.compute(association.getJobId(),
                (jobId, associations) -> {
                    if (associations == null) {
                        associations = Collections.emptySet();
                    }
                    Set<JobLoadBalancer> copy = new HashSet<>(associations);
                    // replace if existing
                    copy.remove(association);
                    copy.add(association);
                    return Collections.unmodifiableSet(copy);
                }
        );
    }

    public void remove(JobLoadBalancer association) {
        tracked.computeIfPresent(association.getJobId(),
                (jobId, associations) -> {
                    final Set<JobLoadBalancer> copy = associations.stream()
                            .filter(entry -> !entry.equals(association))
                            .collect(Collectors.toSet());
                    if (copy.isEmpty()) {
                        return null;
                    }
                    return Collections.unmodifiableSet(copy);
                }
        );
    }

    public Set<String> getAll() {
        return Collections.unmodifiableSet(new HashSet<>(tracked.keySet()));
    }
}
