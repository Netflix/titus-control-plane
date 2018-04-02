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

package com.netflix.titus.common.util.proxy;

import java.lang.reflect.Proxy;

import com.netflix.titus.common.util.proxy.internal.InvocationHandlerBridge;
import com.netflix.titus.common.util.proxy.internal.LoggingInvocationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A builder for {@link LoggingInvocationHandler} proxy.
 */
public final class LoggingProxyBuilder<API, INSTANCE extends API> {

    public enum Priority {ERROR, INFO, DEBUG, NEVER}

    private final Class<API> apiInterface;
    private final INSTANCE instance;

    private Priority requestLevel = Priority.DEBUG;
    private Priority replyLevel = Priority.DEBUG;
    private Priority observableReplyLevel = Priority.DEBUG;
    private Priority exceptionLevel = Priority.ERROR;
    private Priority observableErrorLevel = Priority.ERROR;
    private Logger logger;

    public LoggingProxyBuilder(Class<API> apiInterface, INSTANCE instance) {
        this.apiInterface = apiInterface;
        this.instance = instance;
    }

    public LoggingProxyBuilder<API, INSTANCE> request(Priority level) {
        this.requestLevel = level;
        return this;
    }

    public LoggingProxyBuilder<API, INSTANCE> reply(Priority level) {
        this.replyLevel = level;
        return this;
    }

    public LoggingProxyBuilder<API, INSTANCE> observableReply(Priority level) {
        this.observableReplyLevel = level;
        return this;
    }

    public LoggingProxyBuilder<API, INSTANCE> exception(Priority level) {
        this.exceptionLevel = level;
        return this;
    }

    public LoggingProxyBuilder<API, INSTANCE> observableError(Priority level) {
        this.observableErrorLevel = level;
        return this;
    }

    public LoggingProxyBuilder<API, INSTANCE> logger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public ProxyInvocationHandler buildHandler() {
        if (logger == null) {
            logger = LoggerFactory.getLogger(getCategory(apiInterface, instance));
        }

        return new LoggingInvocationHandler(
                apiInterface,
                logger,
                requestLevel,
                replyLevel,
                observableReplyLevel,
                exceptionLevel,
                observableErrorLevel
        );
    }

    public API build() {
        return (API) Proxy.newProxyInstance(
                apiInterface.getClassLoader(),
                new Class<?>[]{apiInterface},
                new InvocationHandlerBridge<>(buildHandler(), instance)
        );
    }

    static <API, INSTANCE extends API> Class<?> getCategory(Class<API> apiInterface, INSTANCE instance) {
        if (instance == null || instance instanceof java.lang.reflect.Proxy) {
            return apiInterface;
        }
        return instance.getClass();
    }
}
