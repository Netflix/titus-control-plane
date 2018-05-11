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

package com.netflix.titus.common.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Helper reflection functions.
 */
public final class ReflectionExt {

    private static final Set<Class<?>> WRAPPERS = CollectionsExt.asSet(
            Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
            Boolean.class
    );

    private static final Set<Class<?>> STD_DATA_TYPES = CollectionsExt.asSet(
            String.class
    );

    private static final ConcurrentMap<Class<?>, List<Field>> CLASS_FIELDS = new ConcurrentHashMap<>();

    private static final ConcurrentMap<Method, Optional<Method>> FIND_INTERFACE_METHOD_CACHE = new ConcurrentHashMap<>();

    private ReflectionExt() {
    }

    public static boolean isPrimitiveOrWrapper(Class<?> type) {
        if (type.isPrimitive()) {
            return true;
        }
        return WRAPPERS.contains(type);
    }

    public static boolean isStandardDataType(Class<?> type) {
        return isPrimitiveOrWrapper(type) || STD_DATA_TYPES.contains(type);
    }


    public static boolean isContainerType(Field field) {
        Class<?> fieldType = field.getType();
        if (Collection.class.isAssignableFrom(fieldType)) {
            return true;
        }
        if (Map.class.isAssignableFrom(fieldType)) {
            return true;
        }
        return Optional.class.isAssignableFrom(fieldType);
    }

    public static boolean isNumeric(Class<?> type) {
        if (Number.class.isAssignableFrom(type)) {
            return true;
        }
        if (Integer.TYPE == type || Long.TYPE == type || Float.TYPE == type || Double.TYPE == type) {
            return true;
        }
        return false;
    }

    public static List<Class<?>> findAllInterfaces(Class<?> type) {
        List<Class<?>> result = new ArrayList<>();
        Collections.addAll(result, type.getInterfaces());
        if (type.getSuperclass() != Object.class) {
            result.addAll(findAllInterfaces(type.getSuperclass()));
        }
        return result;
    }

    public static <T, A extends Annotation> List<Method> findAnnotatedMethods(T instance, Class<A> methodAnnotation) {
        Class<?> instanceType = bypassBytecodeGeneratedWrappers(instance);

        List<Method> result = new ArrayList<>();
        for (Method m : instanceType.getMethods()) {
            for (Annotation a : m.getAnnotations()) {
                if (a.annotationType() == methodAnnotation) {
                    result.add(m);
                }
            }
        }
        return result;
    }

    public static Optional<StackTraceElement> findCallerStackTrace() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (int i = 3; i < stackTrace.length; i++) {
            StackTraceElement element = stackTrace[i];
            String className = element.getClassName();
            if (!className.contains("cglib")
                    && !className.contains("com.google.inject")
                    && !className.contains("EnhancerByGuice")
                    && !className.contains("ProxyMethodInterceptor")
                    && !className.contains("util.proxy")) {
                return Optional.of(element);
            }
        }
        return Optional.empty();
    }

    private static <T> Class<?> bypassBytecodeGeneratedWrappers(T instance) {
        Class<?> instanceType = instance.getClass();
        while (true) {
            if (instanceType.getName().contains("EnhancerByGuice")) {
                instanceType = instanceType.getSuperclass();
            } else {
                break;
            }
        }
        return instanceType;
    }

    public static Optional<Method> findInterfaceMethod(Method method) {
        return FIND_INTERFACE_METHOD_CACHE.computeIfAbsent(method, m -> {
            if (method.getDeclaringClass().isInterface()) {
                return Optional.of(method);
            }
            for (Class<?> interf : method.getDeclaringClass().getInterfaces()) {
                try {
                    Method interfMethod = interf.getMethod(method.getName(), method.getParameterTypes());
                    if (interfMethod != null) {
                        return Optional.of(interfMethod);
                    }
                } catch (NoSuchMethodException ignore) {
                }
            }
            return Optional.empty();
        });
    }

    public static Field getField(Class<?> type, String name) {
        try {
            return type.getDeclaredField(name);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(format("Class %s has no field %s", type, name));
        }
    }

    public static <T> T getFieldValue(Field field, Object object) {
        try {
            return (T) field.get(object);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(format("Cannot access field %s in %s", field.getName(), object.getClass()));
        }
    }

    public static List<Field> getAllFields(Class<?> type) {
        return CLASS_FIELDS.computeIfAbsent(type, t -> {
            List<Field> fields = new ArrayList<>();
            for (Class<?> current = t; current != Object.class; current = current.getSuperclass()) {
                fields.addAll(asList(current.getDeclaredFields()));
            }
            fields.forEach(f -> f.setAccessible(true));
            return fields;
        });
    }
}
