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

package com.netflix.titus.common.model.sanitizer.internal;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.model.sanitizer.FieldSanitizer;
import com.netflix.titus.common.util.ReflectionExt;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;

import static com.netflix.titus.common.util.ReflectionExt.isNumeric;
import static java.lang.String.format;

/**
 */
public class AnnotationBasedSanitizer extends AbstractFieldSanitizer<Object> {

    private static final SanitizerInfo EMPTY_SANITIZER_INFO = new SanitizerInfo(false, null, null, -1, -1);

    private final static ConcurrentMap<Field, SanitizerInfo> FIELD_SANITIZER_INFOS = new ConcurrentHashMap<>();

    private final ExpressionParser parser = new SpelExpressionParser();
    private final EvaluationContext spelContext;
    private final Function<Class<?>, Boolean> innerEntityPredicate;

    public AnnotationBasedSanitizer(EvaluationContext spelContext,
                                    Function<Class<?>, Boolean> innerEntityPredicate) {
        this.innerEntityPredicate = innerEntityPredicate;
        this.spelContext = spelContext;
    }

    @Override
    public Optional<Object> apply(Object entity) {
        return apply(entity, NOTHING);
    }

    @Override
    protected Optional<Object> sanitizeFieldValue(Field field, Object value, Object context) {
        // If has annotation, sanitize
        SanitizerInfo sanitizerInfo = getSanitizerInfo(field);
        if (sanitizerInfo != EMPTY_SANITIZER_INFO) {
            return sanitizerInfo.isNumeric()
                    ? sanitizeNumericValue(field, value, sanitizerInfo)
                    : sanitizeNotNumericValue(value, sanitizerInfo);
        }

        // If null, do nothing
        if (value == null) {
            return Optional.empty();
        }
        // Skip primitive type or enum or collections/maps/optional
        Class<?> fieldType = field.getType();
        if (ReflectionExt.isStandardDataType(fieldType) || value.getClass().isEnum() || ReflectionExt.isContainerType(field)) {
            return Optional.empty();
        }

        return apply(value, NOTHING);
    }

    private Optional<Object> sanitizeNotNumericValue(Object value, SanitizerInfo sanitizerInfo) {
        if (value == null) {
            return Optional.empty();
        }

        Optional<Object> sanitized;
        if (sanitizerInfo.getAdjusterExpression().isPresent()) {
            sanitized = adjust(value, sanitizerInfo);
        } else {
            sanitized = sanitizerInfo.getSanitizer().apply(value);
        }

        if (!value.getClass().isEnum() && innerEntityPredicate.apply(value.getClass())) {
            if (!sanitized.isPresent()) {
                return apply(value, NOTHING);
            }
            return sanitized.map(v -> apply(v, NOTHING).orElse(v));
        }
        return sanitized;
    }

    private Optional<Object> sanitizeNumericValue(Field field, Object value, SanitizerInfo sanitizerInfo) {
        Class<?> valueType = value.getClass();

        if (valueType == Integer.class || valueType == Integer.TYPE) {
            int effectiveValue = (int) Math.max((int) value, sanitizerInfo.getAtLeast());
            effectiveValue = (int) Math.min(effectiveValue, sanitizerInfo.getAtMost());
            if (sanitizerInfo.getAdjusterExpression() != null) {
                effectiveValue = adjust(effectiveValue, sanitizerInfo).orElse(effectiveValue);
            }
            return effectiveValue == (int) value ? Optional.empty() : Optional.of(effectiveValue);
        } else if (valueType == Long.class || valueType == Long.TYPE) {
            long effectiveValue = Math.max((long) value, sanitizerInfo.getAtLeast());
            effectiveValue = Math.min(effectiveValue, sanitizerInfo.getAtMost());
            if (sanitizerInfo.getAdjusterExpression() != null) {
                effectiveValue = adjust(effectiveValue, sanitizerInfo).orElse(effectiveValue);
            }
            return effectiveValue == (long) value ? Optional.empty() : Optional.of(effectiveValue);
        }

        if (valueType == Float.class || valueType == Float.TYPE) {
            float effectiveValue = Math.max((float) value, sanitizerInfo.getAtLeast());
            effectiveValue = Math.min(effectiveValue, sanitizerInfo.getAtMost());
            if (sanitizerInfo.getAdjusterExpression() != null) {
                effectiveValue = adjust(effectiveValue, sanitizerInfo).orElse(effectiveValue);
            }
            return effectiveValue == (float) value ? Optional.empty() : Optional.of(effectiveValue);
        } else if (valueType == Double.class || valueType == Double.TYPE) {
            double effectiveValue = Math.max((double) value, sanitizerInfo.getAtLeast());
            effectiveValue = Math.min(effectiveValue, sanitizerInfo.getAtMost());
            if (sanitizerInfo.getAdjusterExpression() != null) {
                effectiveValue = adjust(effectiveValue, sanitizerInfo).orElse(effectiveValue);
            }
            return effectiveValue == (double) value ? Optional.empty() : Optional.of(effectiveValue);
        }
        throw new IllegalArgumentException(format("Not support numeric type %s in field %s", valueType, field));
    }

    private <T> Optional<T> adjust(T value, SanitizerInfo sanitizerInfo) {
        return sanitizerInfo.getAdjusterExpression().map(e -> (T) e.getValue(spelContext, new SpELFieldValidator.Root(value)));
    }

    private SanitizerInfo getSanitizerInfo(Field field) {
        return FIELD_SANITIZER_INFOS.computeIfAbsent(field, f -> {
            FieldSanitizer annotation = f.getAnnotation(FieldSanitizer.class);
            return annotation == null ? EMPTY_SANITIZER_INFO : buildSanitizerInfo(field, annotation);
        });
    }

    private SanitizerInfo buildSanitizerInfo(Field field, FieldSanitizer annotation) {
        Function<Object, Optional<Object>> serializer;
        try {
            serializer = annotation.sanitizer().newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot instantiate sanitizer function from " + annotation.sanitizer(), e);
        }

        boolean numeric = isNumeric(field.getType());

        Class<?> emptySanitizerClass = FieldSanitizer.EmptySanitizer.class;
        boolean hasSanitizer = annotation.sanitizer().getClass() == emptySanitizerClass;

        boolean hasAdjuster = !annotation.adjuster().isEmpty();

        if (annotation.atLeast() != Long.MIN_VALUE || annotation.atMost() != Long.MAX_VALUE) {
            Preconditions.checkArgument(numeric, "atLeast/atMost parameters can be used only with numeric types");
            Preconditions.checkArgument(annotation.atLeast() <= annotation.atMost(), "Violated invariant: atLeast <= atMost in field " + field);
        }
        Preconditions.checkArgument(!(hasSanitizer && hasAdjuster), "Sanitizer and adjuster cannot be used at the same time in field: " + field);

        Expression adjusterExpression = !hasAdjuster ? null : parser.parseExpression(annotation.adjuster());
        return new SanitizerInfo(numeric, serializer, Optional.ofNullable(adjusterExpression), annotation.atLeast(), annotation.atMost());
    }

    static class SanitizerInfo {
        private final boolean numeric;
        private final Function<Object, Optional<Object>> sanitizer;
        private final long atLeast;
        private final long atMost;

        private Optional<Expression> adjusterExpression;

        SanitizerInfo(boolean numeric, Function<Object, Optional<Object>> sanitizer, Optional<Expression> adjusterExpression, long atLeast, long atMost) {
            this.numeric = numeric;
            this.sanitizer = sanitizer;
            this.adjusterExpression = adjusterExpression;
            this.atLeast = atLeast;
            this.atMost = atMost;
        }

        boolean isNumeric() {
            return numeric;
        }

        Function<Object, Optional<Object>> getSanitizer() {
            return sanitizer;
        }

        Optional<Expression> getAdjusterExpression() {
            return adjusterExpression;
        }

        long getAtLeast() {
            return atLeast;
        }

        long getAtMost() {
            return atMost;
        }
    }
}
