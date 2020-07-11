/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.mesos;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.titus.common.util.tuple.Pair;

public class TaskAssignments implements Iterable<TaskInfoRequest> {

    private final List<Pair<Machine, List<TaskInfoRequest>>> assignmentsByMachine;
    private final List<TaskInfoRequest> allTasks;

    public TaskAssignments(List<Pair<Machine, List<TaskInfoRequest>>> assignmentsByMachine) {
        this.assignmentsByMachine = assignmentsByMachine;
        // the list below may need shuffling to avoid issues with bias on tasks being ordered by machine
        this.allTasks = assignmentsByMachine.stream()
                .flatMap(pair -> pair.getRight().stream())
                .collect(Collectors.toList());
    }

    public int getCount() {
        return allTasks.size();
    }

    public void forEach(BiConsumer<Machine, List<TaskInfoRequest>> consumer) {
        for (Pair<Machine, List<TaskInfoRequest>> pair : assignmentsByMachine) {
            consumer.accept(pair.getLeft(), pair.getRight());
        }
    }

    @Override
    @Nonnull
    public Iterator<TaskInfoRequest> iterator() {
        return allTasks.iterator();
    }

    public static final class Machine {
        private final String id;
        private final List<VirtualMachineLease> leases;

        public Machine(String id, List<VirtualMachineLease> leases) {
            this.id = id;
            this.leases = leases;
        }

        public String getId() {
            return id;
        }

        public List<VirtualMachineLease> getLeases() {
            return leases;
        }
    }

}
