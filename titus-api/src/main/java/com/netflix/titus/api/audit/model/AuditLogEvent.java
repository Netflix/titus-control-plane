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

package com.netflix.titus.api.audit.model;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AuditLogEvent {

    public enum Type {
        // TODO remove these as we delete the v2 code
        NAMED_JOB_CREATE,
        NAMED_JOB_UPDATE,
        NAMED_JOB_DELETE,
        NAMED_JOB_DISABLED,
        NAMED_JOB_ENABLED,
        JOB_SUBMIT,
        JOB_TERMINATE,
        JOB_DELETE,
        JOB_SCALE_UP,
        JOB_SCALE_DOWN,
        JOB_SCALE_UPDATE,
        JOB_PROCESSES_UPDATE,
        JOB_IN_SERVICE_STATUS_CHANGE,
        WORKER_START,
        WORKER_TERMINATE,
        CLUSTER_SCALE_UP,
        CLUSTER_SCALE_DOWN,
        CLUSTER_ACTIVE_VMS

        // TODO implement the v3 auditing types
    }

    private final Type type;
    private final String operand;
    private final String data;
    private final long time;

    /**
     * As we keep only active state, job/task notifications when delivered to a handler may reference data removed
     * from memory. To avoid costly fetch from archive storage, we include them in the notification object itself.
     * This is a workaround for current V2 engine implementation, which we will fix in V3.
     */
    @JsonIgnore
    private final Optional<Object> sourceRef;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public AuditLogEvent(
            @JsonProperty("type") Type type,
            @JsonProperty("operand") String operand,
            @JsonProperty("data") String data,
            @JsonProperty("time") long time) {
        this.type = type;
        this.operand = operand;
        this.data = data;
        this.time = time;
        this.sourceRef = Optional.empty();
    }

    public AuditLogEvent(Type type, String operand, String data, long time, Object sourceRef) {
        this.type = type;
        this.operand = operand;
        this.data = data;
        this.time = time;
        this.sourceRef = Optional.ofNullable(sourceRef);
    }

    public Type getType() {
        return type;
    }

    public String getOperand() {
        return operand;
    }

    public String getData() {
        return data;
    }

    public long getTime() {
        return time;
    }

    @JsonIgnore
    public Optional<Object> getSourceRef() {
        return sourceRef;
    }

    @Override
    public String toString() {
        return "AuditLogEvent{" +
                "type=" + type +
                ", operand='" + operand + '\'' +
                ", data='" + data + '\'' +
                ", time=" + time +
                ", sourceRef=" + sourceRef.map(r -> r.getClass().getSimpleName()).orElse("none") +
                '}';
    }

    public static <SOURCE> AuditLogEvent of(Type type, String operand, String data, SOURCE sourceRef) {
        return new AuditLogEvent(type, operand, data, System.currentTimeMillis(), sourceRef);
    }
}
