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

package com.netflix.titus.api.endpoint.v2.rest.representation;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.titus.api.audit.model.AuditLogEvent;
import com.netflix.titus.api.audit.model.AuditLogEvent;
import com.netflix.titus.common.util.DateTimeExt;

/**
 */
public class AuditLog {

    private final String type;
    private final String operand;
    private final String data;
    private final String time;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public AuditLog(
            @JsonProperty("type") String type,
            @JsonProperty("operand") String operand,
            @JsonProperty("data") String data,
            @JsonProperty("time") String time) {

        this.type = type;
        this.operand = operand;
        this.data = data;
        this.time = time;
    }

    public static AuditLog fromV2AuditLogEvent(AuditLogEvent auditLogEvent) {
        return new AuditLog(auditLogEvent.getType().name(),
                auditLogEvent.getOperand(),
                auditLogEvent.getData(),
                DateTimeExt.toUtcDateTimeString(auditLogEvent.getTime()));
    }

    public static List<AuditLog> fromV2AuditLogEvent(List<AuditLogEvent> auditLogEvents) {
        final List<AuditLog> auditLogs = new ArrayList<>(auditLogEvents.size());
        auditLogs.addAll(auditLogEvents.stream().map(AuditLog::fromV2AuditLogEvent).collect(Collectors.toList()));
        return auditLogs;
    }

    public String getType() {
        return type;
    }

    public String getOperand() {
        return operand;
    }

    public String getData() {
        return data;
    }

    public String getTime() {
        return time;
    }
}
