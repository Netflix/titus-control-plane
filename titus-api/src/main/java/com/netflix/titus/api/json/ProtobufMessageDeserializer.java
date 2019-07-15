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

package com.netflix.titus.api.json;

import java.io.IOException;
import java.lang.reflect.Method;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public class ProtobufMessageDeserializer extends JsonDeserializer<Message> implements
        ContextualDeserializer {

    private Class<?> type;

    public ProtobufMessageDeserializer() {
    }

    public ProtobufMessageDeserializer(Class<?> type) {
        this.type = type;
    }

    @Override
    public Message deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
        try {
            Method newBuilderMethod = type.getMethod("newBuilder");
            Message.Builder proto = (Message.Builder) newBuilderMethod.invoke(null);
            String jsonString = jsonParser.readValueAsTree().toString();
            JsonFormat.parser().merge(jsonString, proto);
            return proto.build();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext context, BeanProperty property) throws JsonMappingException {
        type = context.getContextualType().getRawClass();
        return new ProtobufMessageDeserializer(type);
    }
}
