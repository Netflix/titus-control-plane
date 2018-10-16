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

package com.netflix.titus.master.supervisor.model.event;

import java.util.Objects;

import com.netflix.titus.master.supervisor.model.MasterInstance;

public class MasterInstanceUpdateEvent extends SupervisorEvent {

    private final MasterInstance masterInstance;

    public MasterInstanceUpdateEvent(MasterInstance masterInstance) {
        this.masterInstance = masterInstance;
    }

    public MasterInstance getMasterInstance() {
        return masterInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MasterInstanceUpdateEvent that = (MasterInstanceUpdateEvent) o;
        return Objects.equals(masterInstance, that.masterInstance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterInstance);
    }

    @Override
    public String toString() {
        return "MasterInstanceUpdateEvent{" +
                "masterInstance=" + masterInstance +
                "} " + super.toString();
    }
}
