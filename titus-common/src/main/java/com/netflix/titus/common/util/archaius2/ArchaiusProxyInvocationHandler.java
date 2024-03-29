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

package com.netflix.titus.common.util.archaius2;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.util.ReflectionExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.unit.TimeUnitExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ArchaiusProxyInvocationHandler implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(ArchaiusProxyInvocationHandler.class);

    private interface MethodHandler {
        Object get() throws Throwable;
    }

    private final Class<?> apiInterface;
    private final String prefix;
    private final MyEnvironment environment;

    private final Map<Method, MethodHandler> methodWrappers;

    ArchaiusProxyInvocationHandler(Class<?> apiInterface, String prefix, MyEnvironment environment) {
        Preconditions.checkArgument(apiInterface.isInterface(), "Not interface: %s", apiInterface);

        this.apiInterface = apiInterface;

        String effectivePrefix = prefix;
        if (prefix == null) {
            Configuration configurationAnnotation = apiInterface.getAnnotation(Configuration.class);
            effectivePrefix = configurationAnnotation != null ? configurationAnnotation.prefix() : null;
        }
        this.prefix = StringExt.isEmpty(effectivePrefix) ? "" : (effectivePrefix.endsWith(".") ? effectivePrefix : effectivePrefix + '.');
        this.environment = environment;

        Map<Method, MethodHandler> methodWrappers = new HashMap<>();
        for (Method method : apiInterface.getMethods()) {
            Preconditions.checkArgument(
                    method.getParameterCount() == 0 || method.isDefault(),
                    "Method with no parameters expected or a default method"
            );
            if (!method.isDefault()) {
                methodWrappers.put(method, new PropertyMethodHandler(method));
            }
        }
        this.methodWrappers = methodWrappers;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.isDefault()) {
            return ReflectionExt.invokeDefault(proxy, apiInterface, method, args);
        }

        MethodHandler wrapper = methodWrappers.get(method);
        if (wrapper != null) {
            return wrapper.get();
        }
        // Must be one of the Object methods.
        return method.invoke(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(apiInterface.getSimpleName()).append('{');

        methodWrappers.forEach((method, handler) -> {
            if (handler instanceof PropertyMethodHandler) {
                builder.append(handler).append(", ");
            }
        });
        builder.setLength(builder.length() - 2);
        builder.append('}');

        return builder.toString();
    }

    static <I> I newProxy(Class<I> apiInterface, String prefix, MyEnvironment environment) {
        Preconditions.checkArgument(apiInterface.isInterface(), "Java interface expected");
        return (I) Proxy.newProxyInstance(
                apiInterface.getClassLoader(),
                new Class[]{apiInterface},
                new ArchaiusProxyInvocationHandler(apiInterface, prefix, environment)
        );
    }

    private class PropertyMethodHandler implements MethodHandler {

        private final String key;
        private final String baseKeyName;
        private final String defaultValue;
        private final Method method;

        private volatile ValueHolder valueHolder;

        private PropertyMethodHandler(Method method) {
            this.method = method;
            this.key = buildKeyName(method);
            this.baseKeyName = buildKeyBaseName(key);

            DefaultValue defaultAnnotation = method.getAnnotation(DefaultValue.class);
            this.defaultValue = defaultAnnotation == null ? null : defaultAnnotation.value();

            this.valueHolder = new ValueHolder(method, environment.getProperty(key, defaultValue));
        }

        @Override
        public Object get() {
            String currentString = environment.getProperty(key, defaultValue);
            if (Objects.equals(currentString, valueHolder.getStringValue())) {
                return valueHolder.getValue();
            }
            try {
                this.valueHolder = new ValueHolder(method, currentString);
            } catch (Exception e) {
                // Do not propagate exception. Return the previous result.
                logger.debug("Bad property value: key={}, value={}", key, currentString);
            }
            return valueHolder.getValue();
        }

        @Override
        public String toString() {
            return baseKeyName + '=' + valueHolder.getValue();
        }

        private String buildKeyName(Method method) {
            PropertyName propertyNameAnnotation = method.getAnnotation(PropertyName.class);
            if (propertyNameAnnotation != null) {
                return StringExt.isEmpty(prefix) ? propertyNameAnnotation.name() : prefix + propertyNameAnnotation.name();
            }

            Either<String, IllegalArgumentException> baseName = StringExt.nameFromJavaBeanGetter(method.getName());
            if (baseName.hasError()) {
                throw baseName.getError();
            }
            return StringExt.isEmpty(prefix) ? baseName.getValue() : prefix + baseName.getValue();
        }

        private String buildKeyBaseName(String key) {
            int idx = key.lastIndexOf('.');
            return idx < 0 ? key : key.substring(idx + 1);
        }
    }

    private static class ValueHolder {

        private final String stringValue;
        private final Object value;

        private ValueHolder(Method method, String stringValue) {
            Class<?> valueType = method.getReturnType();
            Preconditions.checkArgument(!valueType.isPrimitive() || stringValue != null, "Configuration value cannot be null for primitive types");

            this.stringValue = stringValue;

            if (stringValue == null) {
                if (List.class.isAssignableFrom(valueType)) {
                    this.value = Collections.emptyList();
                } else if (Set.class.isAssignableFrom(valueType)) {
                    this.value = Collections.emptySet();
                } else {
                    this.value = null;
                }
            } else if (String.class.equals(valueType)) {
                this.value = stringValue;
            } else if (Long.class.equals(valueType) || long.class.equals(valueType)) {
                this.value = Long.parseLong(stringValue);
            } else if (Integer.class.equals(valueType) || int.class.equals(valueType)) {
                this.value = Integer.parseInt(stringValue);
            } else if (Double.class.equals(valueType) || double.class.equals(valueType)) {
                this.value = Double.parseDouble(stringValue);
            } else if (Float.class.equals(valueType) || float.class.equals(valueType)) {
                this.value = Float.parseFloat(stringValue);
            } else if (Boolean.class.equals(valueType) || boolean.class.equals(valueType)) {
                this.value = Boolean.parseBoolean(stringValue);
            } else if (Duration.class.equals(valueType)) {
                long intervalMs = TimeUnitExt.toMillis(stringValue).orElseThrow(() -> new IllegalArgumentException("Invalid time interval: " + stringValue));
                this.value = Duration.ofMillis(intervalMs);
            } else if (List.class.isAssignableFrom(valueType)) {
                ParameterizedType genericReturnType = (ParameterizedType) method.getGenericReturnType();
                this.value = parseList(genericReturnType.getActualTypeArguments()[0], stringValue);
            } else if (Set.class.isAssignableFrom(valueType)) {
                ParameterizedType genericReturnType = (ParameterizedType) method.getGenericReturnType();
                this.value = new HashSet<>(parseList(genericReturnType.getActualTypeArguments()[0], stringValue));
            } else {
                throw new IllegalArgumentException("Not supported configuration type: " + method);
            }
        }

        public String getStringValue() {
            return stringValue;
        }

        private Object getValue() {
            return value;
        }

        private List<String> parseList(Type elementType, String stringValue) {
            Preconditions.checkArgument(elementType.equals(String.class), "Only List<String> supported");
            return StringExt.splitByComma(stringValue);
        }
    }
}
