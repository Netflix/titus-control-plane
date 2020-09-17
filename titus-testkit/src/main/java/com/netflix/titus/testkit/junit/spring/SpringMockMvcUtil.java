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

package com.netflix.titus.testkit.junit.spring;

import java.util.Collections;
import java.util.Optional;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.runtime.endpoint.metadata.spring.CallMetadataAuthentication;
import com.netflix.titus.runtime.endpoint.v3.rest.RestConstants;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Miscellaneous functions to use in the Spring MVC test setup.
 */
public final class SpringMockMvcUtil {

    public static final CallMetadata JUNIT_REST_CALL_METADATA = CallMetadata.newBuilder()
            .withCallers(Collections.singletonList(Caller.newBuilder()
                    .withId("junitTestCaller")
                    .withCallerType(CallerType.User)
                    .build())
            )
            .build();

    public static final Authentication JUNIT_DELEGATE_AUTHENTICATION = new TestingAuthenticationToken("junitUser", "junitPassword");

    public static final CallMetadataAuthentication JUNIT_AUTHENTICATION = new CallMetadataAuthentication(
            JUNIT_REST_CALL_METADATA,
            JUNIT_DELEGATE_AUTHENTICATION
    );

    public static final Page FIRST_PAGE_OF_1 = pageOf(1);
    public static final Page NEXT_PAGE_OF_1 = pageOf(1, "testCursorPosition");
    public static final Page NEXT_PAGE_OF_2 = pageOf(2, "testCursorPosition");

    public static Page pageOf(int pageSize) {
        return Page.newBuilder().setPageSize(pageSize).build();
    }

    public static Page pageOf(int pageSize, String cursor) {
        return Page.newBuilder().setPageSize(pageSize).setCursor(cursor).build();
    }

    public static Pagination paginationOf(Page current) {
        return Pagination.newBuilder()
                .setCurrentPage(current)
                .setTotalItems(Integer.MAX_VALUE)
                .setHasMore(true)
                .setCursor("testCursor")
                .build();
    }

    public static <E extends Message> E doGet(MockMvc mockMvc, String path, Class<E> entityType) throws Exception {
        return executeGet(mockMvc, newBuilder(entityType), MockMvcRequestBuilders.get(path).principal(JUNIT_AUTHENTICATION));
    }

    public static <E extends Message> E doPaginatedGet(MockMvc mockMvc, String path, Class<E> entityType, Page page, String... queryParameters) throws Exception {
        MockHttpServletRequestBuilder requestBuilder = get(path)
                .queryParam(RestConstants.PAGE_SIZE_QUERY_KEY, "" + page.getPageSize())
                .principal(JUNIT_AUTHENTICATION);
        for (int i = 0; i < queryParameters.length; i += 2) {
            requestBuilder.queryParam(queryParameters[i], queryParameters[i + 1]);
        }

        if (StringExt.isNotEmpty(page.getCursor())) {
            requestBuilder.queryParam(RestConstants.CURSOR_QUERY_KEY, "" + page.getCursor());
        }
        return executeGet(mockMvc, newBuilder(entityType), requestBuilder);
    }

    public static void doPost(MockMvc mockMvc, String path) throws Exception {
        doPost(mockMvc, path, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static <B extends Message> void doPost(MockMvc mockMvc, String path, B body) throws Exception {
        doPost(mockMvc, path, Optional.of(body), Optional.empty(), Optional.empty());
    }

    public static <B extends Message, E extends Message> E doPost(MockMvc mockMvc, String path, B body, Class<E> entityType) throws Exception {
        return doPost(mockMvc, path, Optional.of(body), Optional.of(entityType), Optional.empty()).orElseThrow(() -> new IllegalStateException("Value not provided"));
    }

    public static <B extends Message, E extends Message> Optional<E> doPost(MockMvc mockMvc,
                                                                            String path,
                                                                            Optional<B> bodyOptional,
                                                                            Optional<Class<E>> entityTypeOptional,
                                                                            Optional<String[]> queryParametersOptional) throws Exception {

        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post(path)
                .contentType(MediaType.APPLICATION_JSON)
                .characterEncoding("UTF-8")
                .principal(JUNIT_AUTHENTICATION);

        if (queryParametersOptional.isPresent()) {
            String[] queryParameters = queryParametersOptional.get();
            for (int i = 0; i < queryParameters.length; i += 2) {
                requestBuilder.queryParam(queryParameters[i], queryParameters[i + 1]);
            }
        }

        if (bodyOptional.isPresent()) {
            requestBuilder.content(JsonFormat.printer().print(bodyOptional.get()));
        }

        ResultActions resultActions = mockMvc.perform(requestBuilder)
                .andDo(print())
                .andExpect(r -> {
                    int status = r.getResponse().getStatus();
                    assertThat(status / 100 == 2).describedAs("2xx expected for POST, not: " + status).isTrue();
                });
        if (entityTypeOptional.isPresent()) {
            resultActions.andExpect(content().contentType(MediaType.APPLICATION_JSON));
        }
        MvcResult mvcResult = resultActions.andReturn();

        if (entityTypeOptional.isPresent()) {
            Message.Builder builder = newBuilder(entityTypeOptional.get());
            JsonFormat.parser().merge(mvcResult.getResponse().getContentAsString(), builder);
            return Optional.of((E) builder.build());
        }
        return Optional.empty();
    }

    public static <B extends Message> void doPut(MockMvc mockMvc, String path, B body) throws Exception {
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.put(path)
                .contentType(MediaType.APPLICATION_JSON)
                .characterEncoding("UTF-8")
                .principal(JUNIT_AUTHENTICATION)
                .content(JsonFormat.printer().print(body));
        mockMvc.perform(requestBuilder)
                .andDo(print())
                .andExpect(status().isOk() )
                .andReturn();
    }

    public static void doDelete(MockMvc mockMvc, String path, String... queryParameters) throws Exception {
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.delete(path)
                .contentType(MediaType.APPLICATION_JSON)
                .characterEncoding("UTF-8")
                .principal(JUNIT_AUTHENTICATION);
        for (int i = 0; i < queryParameters.length; i += 2) {
            requestBuilder.queryParam(queryParameters[i], queryParameters[i + 1]);
        }
        mockMvc.perform(requestBuilder)
                .andDo(print())
                .andExpect(status().isOk())
                .andReturn();
    }

    private static <E extends Message> E executeGet(MockMvc mockMvc, Message.Builder builder, MockHttpServletRequestBuilder requestBuilder) throws Exception {
        MvcResult mvcResult = mockMvc.perform(requestBuilder)
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andReturn();

        JsonFormat.parser().merge(mvcResult.getResponse().getContentAsString(), builder);
        return (E) builder.build();
    }

    private static <E> Message.Builder newBuilder(Class<E> entityType) {
        try {
            Message defaultInstance = (Message) entityType.getDeclaredMethod("getDefaultInstance").invoke(null, new Object[0]);
            return defaultInstance.toBuilder();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create builder for type: " + entityType, e);
        }
    }
}
