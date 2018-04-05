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

package com.netflix.titus.testkit.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import static com.netflix.titus.api.json.ObjectMappers.compactMapper;


public final class PrettyPrinters {

    public static String print(MessageOrBuilder message) {
        try {
            return JsonFormat.printer().print(message);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Protobuf to JSON formatting error", e);
        }
    }

    public static String printCompact(MessageOrBuilder message) {
        try {
            JsonNode jsonNode = compactMapper().readTree(JsonFormat.printer().print(message));
            return compactMapper().writeValueAsString(jsonNode);
        } catch (Exception e) {
            throw new IllegalArgumentException("Protobuf to JSON formatting error", e);
        }
    }
}
