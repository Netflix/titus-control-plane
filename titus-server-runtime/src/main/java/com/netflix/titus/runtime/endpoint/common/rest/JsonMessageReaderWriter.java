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

package com.netflix.titus.runtime.endpoint.common.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.Deserializers;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.google.protobuf.Message;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.runtime.common.json.AssignableFromDeserializers;
import com.netflix.titus.runtime.common.json.CompositeDeserializers;
import com.netflix.titus.runtime.common.json.CustomDeserializerSimpleModule;
import com.netflix.titus.runtime.common.json.ProtobufMessageDeserializer;
import com.netflix.titus.runtime.common.json.ProtobufMessageSerializer;
import com.netflix.titus.runtime.common.json.TrimmingStringDeserializer;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JsonMessageReaderWriter implements MessageBodyReader<Object>, MessageBodyWriter<Object> {

    /**
     * If 'debug' parameter is included in query, include error context when printing {@link ErrorResponse}.
     */
    static final String DEBUG_PARAM = "debug";

    /**
     * if 'fields' parameter is included in a query, only the requested field values are returned.
     */
    static final String FIELDS_PARAM = "fields";

    private static final ObjectMapper MAPPER = createObjectMapper();

    private static final ObjectWriter COMPACT_ERROR_WRITER = MAPPER.writer().withView(ObjectMappers.PublicView.class);

    private static final Validator VALIDATION = Validation.buildDefaultValidatorFactory().getValidator();

    @Context
            /* Visible for testing */ HttpServletRequest httpServletRequest;

    private static ObjectMapper createObjectMapper() {
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

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE);
    }

    @Override
    public boolean isWriteable(Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return mediaType.isCompatible(MediaType.APPLICATION_JSON_TYPE);
    }

    @Override
    public long getSize(Object o, Class type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public Object readFrom(Class<Object> type,
                           Type genericType,
                           Annotation[] annotations,
                           MediaType mediaType,
                           MultivaluedMap<String, String> httpHeaders,
                           InputStream entityStream) throws IOException, WebApplicationException {
        Object entity = MAPPER.readValue(entityStream, type);
        Set<ConstraintViolation<Object>> constraintViolations = VALIDATION.validate(entity);
        if (!constraintViolations.isEmpty()) {
            throw new ConstraintViolationException(type.getSimpleName() +
                    " in request body is incomplete or contains invalid data", constraintViolations);
        }

        return entity;
    }

    @Override
    public void writeTo(Object entity,
                        Class type,
                        Type genericType,
                        Annotation[] annotations,
                        MediaType mediaType,
                        MultivaluedMap httpHeaders,
                        OutputStream entityStream) throws IOException, WebApplicationException {

        // Unless 'debug' flag is set, do not write error context
        if (entity instanceof ErrorResponse) {
            String debug = httpServletRequest.getParameter(DEBUG_PARAM);
            if (debug == null || debug.equalsIgnoreCase("false")) {
                COMPACT_ERROR_WRITER.writeValue(entityStream, entity);
                return;
            }
        }

        List<String> fields = StringExt.splitByComma(httpServletRequest.getParameter(FIELDS_PARAM));
        if (fields.isEmpty()) {
            MAPPER.writeValue(entityStream, entity);
        } else {
            ObjectMappers.applyFieldsFilter(MAPPER, fields).writeValue(entityStream, entity);
        }
    }
}
