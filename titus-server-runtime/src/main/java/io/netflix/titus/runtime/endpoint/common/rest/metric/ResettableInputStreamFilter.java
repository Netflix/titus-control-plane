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

package io.netflix.titus.runtime.endpoint.common.rest.metric;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import com.google.common.io.ByteStreams;
import io.netflix.titus.runtime.endpoint.common.rest.RestServerConfiguration;

@Singleton
public class ResettableInputStreamFilter implements Filter {

    private final RestServerConfiguration config;

    @Inject
    public ResettableInputStreamFilter(RestServerConfiguration config) {
        this.config = config;
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        if (!config.isJaxrsErrorLoggingEnabled()) {
            chain.doFilter(request, response);
        } else {
            chain.doFilter(new ResettableStreamHttpServletRequest((HttpServletRequest) request), response);
        }
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
    }

    private static class ResettableStreamHttpServletRequest extends HttpServletRequestWrapper {

        private HttpServletRequest request;
        private ResettableServletInputStream servletStream;

        public ResettableStreamHttpServletRequest(HttpServletRequest request) {
            super(request);
            this.request = request;
            this.servletStream = new ResettableServletInputStream();
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            if (servletStream.rawData == null) {
                servletStream.rawData = ByteStreams.toByteArray(request.getInputStream());
                servletStream.reset();
            }
            return servletStream;
        }

        @Override
        public BufferedReader getReader() throws IOException {
            if (servletStream.rawData == null) {
                servletStream.rawData = ByteStreams.toByteArray(request.getInputStream());
                servletStream.reset();
            }
            return new BufferedReader(new InputStreamReader(servletStream));
        }

        /**
         * A simplified resettable input stream using a byte array
         */
        private class ResettableServletInputStream extends ServletInputStream {
            private byte[] rawData;
            private InputStream stream;

            @Override
            public int read() throws IOException {
                return stream.read();
            }

            @Override
            public void reset() throws IOException {
                if (rawData == null) {
                    throw new IOException("reset not supported");
                }

                stream = new ByteArrayInputStream(rawData);
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public void close() throws IOException {
                rawData = null;
                stream.close();
            }

            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setReadListener(ReadListener readListener) {
            }
        }
    }
}
