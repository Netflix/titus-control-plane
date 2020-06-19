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

import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.common.util.jackson.CommonObjectMappers;
import com.netflix.titus.testing.SampleGrpcService.SampleComplexMessage;
import com.netflix.titus.testing.SampleGrpcService.SampleComplexMessage.SampleInternalMessage;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest()
@ContextConfiguration(classes = {SampleSpringResource.class, CommonExceptionHandlers.class, TitusProtobufHttpMessageConverter.class})
public class TitusProtobufHttpMessageConverterTest {

    private static final SampleComplexMessage SAMPLE_BASE = SampleComplexMessage.newBuilder()
            .putMapValue("keyA", "valueA")
            .setInternalMessage(SampleInternalMessage.newBuilder()
                    .setIntervalValue("internaValue123")
                    .build()
            )
            .build();

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void testSerializer() throws Exception {
        doPost(SAMPLE_BASE);
        SampleComplexMessage result = doGet("/test");
        assertThat(result).isEqualTo(SAMPLE_BASE);
    }

    /**
     * This is something protobuf {@link JsonFormat} does not do, but Jackson does, and some existing clients
     * depend on it.
     */
    @Test
    public void testEmptyMapsAreSerialized() throws Exception {
        doPost(SampleComplexMessage.getDefaultInstance());
        String result = doGetString("/test");
        assertThat(result).contains("mapValue");
    }

    @Test
    public void testErrors() throws Exception {
        // None debug mode
        ErrorResponse error = doGetError("/test/error");
        assertThat(error.getStatusCode()).isEqualTo(500);
        assertThat(error.getErrorContext()).isNull();

        // Debug mode
        ErrorResponse errorWithContext = doGetError("/test/error?debug=true");
        assertThat(errorWithContext.getStatusCode()).isEqualTo(500);
        assertThat(errorWithContext.getErrorContext()).hasSize(3);
    }

    private String doGetString(String baseUri) throws Exception {
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get(baseUri);
        MvcResult mvcResult = mockMvc.perform(requestBuilder)
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();

        return mvcResult.getResponse().getContentAsString();
    }

    private SampleComplexMessage doGet(String baseUri) throws Exception {
        SampleComplexMessage.Builder resultBuilder = SampleComplexMessage.newBuilder();
        JsonFormat.parser().merge(doGetString(baseUri), resultBuilder);
        return resultBuilder.build();
    }

    private ErrorResponse doGetError(String baseUri) throws Exception {
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.get(baseUri);
        MvcResult mvcResult = mockMvc.perform(requestBuilder)
                .andDo(print())
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();
        return CommonObjectMappers.protobufMapper().readValue(mvcResult.getResponse().getContentAsString(), ErrorResponse.class);
    }

    private void doPost(SampleComplexMessage sample) throws Exception {
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/test")
                .contentType(MediaType.APPLICATION_JSON_VALUE)
                .content(JsonFormat.printer().print(sample));

        mockMvc.perform(requestBuilder)
                .andDo(print())
                .andExpect(status().isCreated())
                .andReturn();
    }
}