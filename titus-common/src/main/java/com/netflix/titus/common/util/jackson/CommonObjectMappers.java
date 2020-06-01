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

package com.netflix.titus.common.util.jackson;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.netflix.titus.common.util.PropertiesExt;
import com.netflix.titus.common.util.ReflectionExt;
import com.netflix.titus.common.util.jackson.internal.AssignableFromDeserializers;
import com.netflix.titus.common.util.jackson.internal.CompositeDeserializers;
import com.netflix.titus.common.util.jackson.internal.CustomDeserializerSimpleModule;
import com.netflix.titus.common.util.jackson.internal.ProtobufMessageDeserializer;
import com.netflix.titus.common.util.jackson.internal.ProtobufMessageSerializer;
import com.netflix.titus.common.util.jackson.internal.TrimmingStringDeserializer;
import rx.exceptions.Exceptions;

/**
 * Jackon's {@link ObjectMapper} is thread safe, and uses cache for optimal performance. It makes sense
 * to reuse the same instance within single JVM. This class provides shared, pre-configured instances of
 * {@link ObjectMapper} with different configuration options.
 */
public class CommonObjectMappers {

    /**
     * A helper marker class for use with {@link JsonView} annotation.
     */
    public static final class PublicView {
    }

    public static final class DebugView {
    }

    private static final ObjectMapper JACKSON_DEFAULT = new ObjectMapper();
    private static final ObjectMapper DEFAULT = createDefaultMapper();
    private static final ObjectMapper COMPACT = createCompactMapper();
    private static final ObjectMapper PROTOBUF = createProtobufMapper();

    public static ObjectMapper jacksonDefaultMapper() {
        return JACKSON_DEFAULT;
    }

    public static ObjectMapper defaultMapper() {
        return DEFAULT;
    }

    public static ObjectMapper compactMapper() {
        return COMPACT;
    }

    public static ObjectMapper protobufMapper() {
        return PROTOBUF;
    }

    public static String writeValueAsString(ObjectMapper objectMapper, Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    public static <T> T readValue(ObjectMapper objectMapper, String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (IOException e) {
            throw Exceptions.propagate(e);
        }
    }

    /**
     * Serializes only the specified fields in Titus POJOs.
     */
    public static ObjectMapper applyFieldsFilter(ObjectMapper original, Collection<String> fields) {
        Preconditions.checkArgument(!fields.isEmpty(), "Fields filter, with no field names provided");

        PropertiesExt.PropertyNode<Boolean> rootNode = PropertiesExt.fullSplit(fields);
        SimpleModule module = new SimpleModule() {
            @Override
            public void setupModule(SetupContext context) {
                super.setupModule(context);
                context.appendAnnotationIntrospector(new TitusAnnotationIntrospector());
            }
        };
        ObjectMapper newMapper = original.copy().registerModule(module);

        SimpleBeanPropertyFilter filter = new SimpleBeanPropertyFilter() {

            private PropertiesExt.PropertyNode<Boolean> findNode(JsonStreamContext outputContext) {
                if (outputContext.inArray()) {
                    return findNode(outputContext.getParent());
                }
                if (outputContext.getParent() == null) {
                    return rootNode;
                }
                PropertiesExt.PropertyNode<Boolean> node = findNode(outputContext.getParent());
                if (node == null) {
                    return null;
                }
                if (isEnabled(node)) {
                    return node;
                }
                return node.getChildren().get(outputContext.getCurrentName());
            }

            @Override
            public void serializeAsField(Object pojo, JsonGenerator jgen, SerializerProvider provider, PropertyWriter writer) throws Exception {
                PropertiesExt.PropertyNode<Boolean> selectorNode = findNode(jgen.getOutputContext().getParent());
                if (selectorNode != null) {
                    boolean enabled = isEnabled(selectorNode);
                    if (!enabled) {
                        PropertiesExt.PropertyNode<Boolean> childNode = selectorNode.getChildren().get(writer.getName());
                        if (childNode != null) {
                            boolean isNested = !childNode.getChildren().isEmpty() && !isPrimitive(writer);
                            enabled = isNested || isEnabled(childNode);
                        }
                    }
                    if (enabled) {
                        writer.serializeAsField(pojo, jgen, provider);
                        return;
                    }
                }
                if (!jgen.canOmitFields()) {
                    writer.serializeAsOmittedField(pojo, jgen, provider);
                }
            }

            private boolean isPrimitive(PropertyWriter writer) {
                if (writer instanceof BeanPropertyWriter) {
                    BeanPropertyWriter bw = (BeanPropertyWriter) writer;
                    return ReflectionExt.isPrimitiveOrWrapper(bw.getType().getRawClass());
                }
                return false;
            }

            private Boolean isEnabled(PropertiesExt.PropertyNode<Boolean> childNode) {
                return childNode.getValue().orElse(Boolean.FALSE);
            }
        };

        newMapper.setFilterProvider(new SimpleFilterProvider().addFilter("titusFilter", filter));
        return newMapper;
    }

    private static class TitusAnnotationIntrospector extends AnnotationIntrospector {
        @Override
        public Version version() {
            return Version.unknownVersion();
        }

        @Override
        public Object findFilterId(Annotated ann) {
            Object id = super.findFilterId(ann);
            if (id == null) {
                String name = ann.getRawType().getName();
                // TODO We hardcode here two packages. It should be configurable.
                if (name.startsWith("com.netflix.titus") || name.startsWith("com.netflix.managedbatch")) {
                    id = "titusFilter";
                }
            }
            return id;
        }
    }

    private static ObjectMapper createDefaultMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
        objectMapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);

        return objectMapper;
    }

    private static ObjectMapper createCompactMapper() {
        return new ObjectMapper();
    }

    private static ObjectMapper createProtobufMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Serialization
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // Deserialization
        mapper.disable(SerializationFeature.INDENT_OUTPUT);

        SimpleDeserializers simpleDeserializers = new SimpleDeserializers();
        simpleDeserializers.addDeserializer(String.class, new TrimmingStringDeserializer());

        List<Deserializers> deserializersList = Arrays.asList(
                new AssignableFromDeserializers(Message.class, new ProtobufMessageDeserializer()),
                simpleDeserializers
        );
        CompositeDeserializers compositeDeserializers = new CompositeDeserializers(deserializersList);
        CustomDeserializerSimpleModule module = new CustomDeserializerSimpleModule(compositeDeserializers);
        module.addSerializer(Message.class, new ProtobufMessageSerializer());
        mapper.registerModule(module);

        return mapper;
    }
}
