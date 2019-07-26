/*
 * Copyright 2019 Netflix, Inc.
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

public class LeaseRescindedEvent {

    private static LeaseRescindedEvent ALL_EVENT = new LeaseRescindedEvent(Type.All, "");

    public enum Type {
        All,
        LeaseId
    }

    private final Type type;
    private final String value;

    private LeaseRescindedEvent(Type type, String value) {
        this.type = type;
        this.value = value;
    }

    public Type getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public static LeaseRescindedEvent allEvent() {
        return ALL_EVENT;
    }

    public static LeaseRescindedEvent leaseIdEvent(String leaseId) {
        return new LeaseRescindedEvent(Type.LeaseId, leaseId);
    }
}
