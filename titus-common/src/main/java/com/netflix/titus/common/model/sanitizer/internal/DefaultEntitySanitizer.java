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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.validation.ConstraintValidator;
import javax.validation.Validation;
import javax.validation.Validator;

import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.ValidationError;
import com.netflix.titus.common.model.sanitizer.VerifierMode;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.support.ReflectiveMethodResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class DefaultEntitySanitizer implements EntitySanitizer {

    private final Validator validator;

    private final List<Function<Object, Optional<Object>>> sanitizers;

    public DefaultEntitySanitizer(VerifierMode verifierMode,
                                  List<Function<Object, Optional<Object>>> sanitizers,
                                  boolean annotationSanitizersEnabled,
                                  boolean stdValueSanitizersEnabled,
                                  Function<Class<?>, Boolean> includesPredicate,
                                  Function<String, Optional<Object>> templateResolver,
                                  Map<String, Method> registeredFunctions,
                                  Map<String, Object> registeredBeans,
                                  Function<Class<?>, Optional<ConstraintValidator<?, ?>>> applicationValidatorFactory) {

        Supplier<EvaluationContext> spelContextFactory = () -> {
            StandardEvaluationContext context = new StandardEvaluationContext();
            registeredFunctions.forEach(context::registerFunction);
            context.setBeanResolver((ctx, beanName) -> registeredBeans.get(beanName));
            context.setMethodResolvers(Collections.singletonList(new ReflectiveMethodResolver()));
            return context;
        };

        this.validator = Validation.buildDefaultValidatorFactory()
                .usingContext()
                .constraintValidatorFactory(new ConstraintValidatorFactoryWrapper(verifierMode, applicationValidatorFactory, spelContextFactory))
                .messageInterpolator(new SpELMessageInterpolator(spelContextFactory))
                .getValidator();

        List<Function<Object, Optional<Object>>> allSanitizers = new ArrayList<>();
        if (annotationSanitizersEnabled) {
            allSanitizers.add(new AnnotationBasedSanitizer(spelContextFactory.get(), includesPredicate));
        }
        if (stdValueSanitizersEnabled) {
            allSanitizers.add(new StdValueSanitizer(includesPredicate));
        }
        allSanitizers.add(new TemplateSanitizer(templateResolver, includesPredicate));
        allSanitizers.addAll(sanitizers);
        this.sanitizers = allSanitizers;
    }

    @Override
    public <T> Set<ValidationError> validate(T entity) {
        return validator.validate(entity).stream()
                .map(violation -> new ValidationError(violation.getPropertyPath().toString(), violation.getMessage()))
                .collect(Collectors.toSet());
    }

    @Override
    public <T> Optional<T> sanitize(T entity) {
        Object sanitized = entity;
        for (Function<Object, Optional<Object>> sanitizer : sanitizers) {
            sanitized = sanitizer.apply(sanitized).orElse(sanitized);
        }
        return sanitized == entity ? Optional.empty() : Optional.of((T) sanitized);
    }
}
