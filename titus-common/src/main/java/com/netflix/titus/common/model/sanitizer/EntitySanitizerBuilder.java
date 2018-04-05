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

package com.netflix.titus.common.model.sanitizer;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.validation.ConstraintValidator;

import com.netflix.titus.common.model.sanitizer.internal.DefaultEntitySanitizer;

/**
 * {@link EntitySanitizer} builder.
 */
public class EntitySanitizerBuilder {

    public interface SanitizationFunction<T> extends Function<T, Optional<Object>> {
    }

    private Function<Class<?>, Optional<ConstraintValidator<?, ?>>> applicationValidatorFactory = type -> Optional.empty();

    private VerifierMode verifierMode = VerifierMode.Permissive;
    private final List<Function<Object, Optional<Object>>> sanitizers = new ArrayList<>();
    private Function<String, Optional<Object>> templateResolver = path -> Optional.empty();
    private boolean annotationSanitizersEnabled;
    private boolean stdValueSanitizersEnabled;

    private Function<Class<?>, Boolean> includesPredicate = type -> false;

    private final Map<String, Method> registeredFunctions = new HashMap<>();
    private final Map<String, Object> registeredBeans = new HashMap<>();

    private EntitySanitizerBuilder() {
    }

    public EntitySanitizerBuilder verifierMode(VerifierMode verifierMode) {
        this.verifierMode = verifierMode;
        return this;
    }

    /**
     * For nested objects to be validated recursively they must be explicitly registered with the {@link EntitySanitizer}.
     * This is accomplished by registering one or more predicate functions. A predicate function returns true
     * if an entity should be validated, false otherwise. Multiple predicates are joined by logical 'or' operator.
     */
    public EntitySanitizerBuilder processEntities(Function<Class<?>, Boolean> includesPredicate) {
        final Function<Class<?>, Boolean> previous = includesPredicate;
        this.includesPredicate = type -> previous.apply(type) || includesPredicate.apply(type);
        return this;
    }

    public EntitySanitizerBuilder addValidatorFactory(Function<Class<?>, Optional<ConstraintValidator<?, ?>>> newValidatorFactory) {
        Function<Class<?>, Optional<ConstraintValidator<?, ?>>> previous = applicationValidatorFactory;
        this.applicationValidatorFactory = type -> {
            Optional<ConstraintValidator<?, ?>> result = previous.apply(type);
            if (!result.isPresent()) {
                return newValidatorFactory.apply(type);
            }
            return result;
        };
        return this;
    }

    /**
     * Add a new data sanitizer. Sanitizers are executed in the same order as they are added.
     */
    public EntitySanitizerBuilder addSanitizer(SanitizationFunction<Object> sanitizer) {
        sanitizers.add(entity -> Optional.of(sanitizer.apply(entity)));
        return this;
    }

    /**
     * Add a new data sanitizer for a specific data type.
     */
    public <T> EntitySanitizerBuilder addSanitizer(Class<T> type, SanitizationFunction<T> typeSanitizer) {
        sanitizers.add(entity ->
                entity.getClass().isAssignableFrom(type) ? typeSanitizer.apply((T) entity) : Optional.empty()
        );
        return this;
    }

    public EntitySanitizerBuilder enableAnnotationSanitizers() {
        this.annotationSanitizersEnabled = true;
        return this;
    }

    /**
     * Enables basic data type sanitization:
     * <ul>
     * <li>string - trim or replace null with empty string</li>
     * </ul>
     */
    public EntitySanitizerBuilder enableStdValueSanitizers() {
        this.stdValueSanitizersEnabled = true;
        return this;
    }

    /**
     * Adding template objects, implicitly enables template based sanitization. If a sanitized entity misses a value, the
     * value will be copied from its corresponding template.
     */
    public EntitySanitizerBuilder addTemplateResolver(Function<String, Optional<Object>> templateResolver) {
        Function<String, Optional<Object>> previous = templateResolver;
        this.templateResolver = path -> {
            Optional<Object> result = previous.apply(path);
            if (!result.isPresent()) {
                return templateResolver.apply(path);
            }
            return result;
        };
        return this;
    }

    /**
     * Make the function available via the provided alias.
     */
    public EntitySanitizerBuilder registerFunction(String alias, Method function) {
        registeredFunctions.put(alias, function);
        return this;
    }

    /**
     * Make the JavaBean available via the provided alias.
     */
    public EntitySanitizerBuilder registerBean(String alias, Object object) {
        registeredBeans.put(alias, object);
        return this;
    }

    public EntitySanitizer build() {
        return new DefaultEntitySanitizer(verifierMode, sanitizers, annotationSanitizersEnabled, stdValueSanitizersEnabled,
                includesPredicate, templateResolver, registeredFunctions, registeredBeans, applicationValidatorFactory);
    }

    /**
     * Empty {@link EntitySanitizerBuilder}.
     */
    public static EntitySanitizerBuilder newBuilder() {
        return new EntitySanitizerBuilder();
    }

    /**
     * Pre-initialized {@link EntitySanitizerBuilder} with all standard features enabled.
     */
    public static EntitySanitizerBuilder stdBuilder() {
        return new EntitySanitizerBuilder()
                .enableStdValueSanitizers()
                .enableAnnotationSanitizers();
    }
}
