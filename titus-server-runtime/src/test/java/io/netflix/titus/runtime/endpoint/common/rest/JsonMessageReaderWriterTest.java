/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.runtime.endpoint.common.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;

import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JsonMessageReaderWriterTest {

    private static final ErrorResponse ERROR_RESPONSE = ErrorResponse.newError(404, "simulated error")
            .withContext("testContext", singletonMap("key", "value"))
            .build();

    private final HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);

    private final JsonMessageReaderWriter provider = new JsonMessageReaderWriter();

    @Before
    public void setUp() throws Exception {
        provider.httpServletRequest = httpServletRequest;
    }

    @Test
    public void testErrorMessageWrittenInCompactForm() throws Exception {
        String jsonText = serialize(ERROR_RESPONSE, ErrorResponse.class);
        assertThat(jsonText).doesNotContain("errorContext");
    }

    @Test
    public void testErrorMessageWrittenInFullForm() throws Exception {
        when(httpServletRequest.getParameter(JsonMessageReaderWriter.DEBUG_PARAM)).thenReturn("true");
        String jsonText = serialize(ERROR_RESPONSE, ErrorResponse.class);
        assertThat(jsonText).contains("errorContext");
    }

    @Test
    public void testFieldsSelection() throws Exception {
        TitusJobSpec myJob = new TitusJobSpec.Builder()
                .appName("appName")
                .type(TitusJobType.batch)
                .applicationName("imageName")
                .build();

        when(httpServletRequest.getParameter(JsonMessageReaderWriter.FIELDS_PARAM)).thenReturn("appName,type");
        String jsonText = serialize(myJob, TitusJobSpec.class);

        assertThat(jsonText).contains("appName", "type");
    }

    private <T> String serialize(T entity, Class<T> type) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        provider.writeTo(entity, type, null, null, null, null, output);
        return new String(output.toByteArray());
    }
}