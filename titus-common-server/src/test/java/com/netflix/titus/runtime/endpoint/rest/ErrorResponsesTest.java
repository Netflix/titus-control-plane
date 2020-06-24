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
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.PropertyBindingException;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ErrorResponsesTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testBuildOfHttpRequest() {
        HttpServletRequest httpRequest = mock(HttpServletRequest.class);
        when(httpRequest.getServletPath()).thenReturn("/servletPath");
        when(httpRequest.getPathInfo()).thenReturn("/pathInfo");
        when(httpRequest.getHeaderNames()).thenReturn(enumerationOf("Content-Type"));

        Map<String, Object> mappedRequest = ErrorResponses.buildHttpRequestContext(httpRequest);

        assertThat(mappedRequest.get("relativeURI"), is(equalTo("/servletPath/pathInfo")));
    }

    @Test
    public void testBuildOfServerContext() {
        Map<String, Object> serverContext = ErrorResponses.buildServerContext();
        assertThat(serverContext.isEmpty(), is(false));
    }

    @Test
    public void testBuildOfGenericException() {
        List<ErrorResponses.StackTraceRepresentation> mappedError;
        try {
            throw new IOException("simulated I/O error");
        } catch (IOException e) {
            mappedError = ErrorResponses.buildExceptionContext(e);
        }
        assertThat(mappedError, is(hasSize(1)));
    }

    @Test
    public void testBuildOfJacksonWithInvalidType() throws Exception {
        String badDocument = "{\"value\":\"notANumber\"}";

        List<ErrorResponses.StackTraceRepresentation> mappedError = null;
        try {
            mapper.readValue(badDocument, MyService.class);
            fail("Expected parsing exception");
        } catch (InvalidFormatException e) {
            mappedError = ErrorResponses.buildExceptionContext(e);
        }

        Map<String, Object> details = mappedError.get(0).getDetails();
        assertThat(details, is(notNullValue()));

        assertThat(details.get("pathReference"), is(equalTo(this.getClass().getName() + "$MyService[\"value\"]")));
        assertThat(details.get("targetType"), is(equalTo("int")));
        assertThat((String) details.get("errorLocation"), containsString("line: 1"));
        assertThat(details.get("document"), is(equalTo("{\"value\":\"notANumber\"}")));
    }

    @Test
    public void testBuildOfJacksonWithInvalidPropertyName() throws Exception {
        String badDocument = "{\"myType\":\"myServiceType\"}";

        List<ErrorResponses.StackTraceRepresentation> mappedError = null;
        try {
            mapper.readValue(badDocument, MyService.class);
            fail("Expected parsing exception");
        } catch (PropertyBindingException e) {
            mappedError = ErrorResponses.buildExceptionContext(e);
        }
        ErrorResponses.StackTraceRepresentation stackTrace = mappedError.get(0);
        assertThat(stackTrace.getDetails(), is(notNullValue()));
        assertThat(stackTrace.getDetails().get("property"), is(equalTo("myType")));
    }

    @Test
    public void testBuildOfThreadContext() throws Exception {
        ErrorResponse errorResponse = ErrorResponse.newError(500).debug(true).threadContext().build();
        List<String> threadContext = (List<String>) errorResponse.getErrorContext().get(ErrorResponse.THREAD_CONTEXT);
        assertThat(threadContext.get(0), containsString(ErrorResponsesTest.class.getName()));
    }

    private Enumeration<String> enumerationOf(String header) {
        return new Enumeration<String>() {

            private boolean sent;

            @Override
            public boolean hasMoreElements() {
                return sent;
            }

            @Override
            public String nextElement() {
                if (sent) {
                    throw new NoSuchElementException();
                }
                sent = true;
                return header;
            }
        };
    }

    static class MyService {

        private int value;

        @JsonCreator()
        MyService(@JsonProperty("value") int value) {
            this.value = value;
        }
    }
}
