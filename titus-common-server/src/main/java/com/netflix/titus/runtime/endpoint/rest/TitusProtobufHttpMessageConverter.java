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

package com.netflix.titus.runtime.endpoint.rest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.protobuf.Message;
import com.netflix.titus.common.util.ProtobufExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.jackson.CommonObjectMappers;
import io.grpc.reflection.v1alpha.ErrorResponse;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.http.converter.protobuf.ProtobufHttpMessageConverter;
import org.springframework.stereotype.Controller;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

@Controller
public class TitusProtobufHttpMessageConverter implements HttpMessageConverter<Message> {

    /**
     * If 'debug' parameter is included in query, include error context when printing {@link ErrorResponse}.
     */
    static final String DEBUG_PARAM = "debug";

    /**
     * if 'fields' parameter is included in a query, only the requested field values are returned.
     */
    static final String FIELDS_PARAM = "fields";

    private static final ObjectMapper MAPPER = CommonObjectMappers.protobufMapper();

    private static final ObjectWriter COMPACT_ERROR_WRITER = MAPPER.writer().withView(CommonObjectMappers.PublicView.class);

    private final HttpMessageConverter<Message> delegate;

    @Inject
    public TitusProtobufHttpMessageConverter() {
        this.delegate = new ProtobufHttpMessageConverter();
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return Message.class.isAssignableFrom(clazz);
    }

    @Override
    public boolean canWrite(Class<?> clazz, MediaType mediaType) {
        return Message.class.isAssignableFrom(clazz) && MediaType.APPLICATION_JSON.equals(mediaType);
    }

    @Override
    public List<MediaType> getSupportedMediaTypes() {
        return Collections.singletonList(MediaType.APPLICATION_JSON);
    }

    @Override
    public Message read(Class<? extends Message> clazz, HttpInputMessage inputMessage) throws IOException, HttpMessageNotReadableException {
        return delegate.read(clazz, inputMessage);
    }

    @Override
    public void write(Message entity, MediaType contentType, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        HttpServletRequest httpServletRequest = ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getRequest();

        outputMessage.getHeaders().add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        // Unless 'debug' flag is set, do not write error context
        if (entity instanceof ErrorResponse) {
            String debug = httpServletRequest.getParameter(DEBUG_PARAM);
            if (debug == null || debug.equalsIgnoreCase("false")) {
                COMPACT_ERROR_WRITER.writeValue(outputMessage.getBody(), entity);
                return;
            }
        }

        List<String> fields = StringExt.splitByComma(httpServletRequest.getParameter(FIELDS_PARAM));
        if (fields.isEmpty()) {
            MAPPER.writeValue(outputMessage.getBody(), entity);
        } else {
            Message filteredEntity = ProtobufExt.copy(entity, new HashSet<>(fields));
            MAPPER.writeValue(outputMessage.getBody(), filteredEntity);
        }
    }
}
