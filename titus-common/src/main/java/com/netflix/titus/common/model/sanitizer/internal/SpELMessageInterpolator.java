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

import java.util.Locale;
import java.util.function.Supplier;
import javax.validation.MessageInterpolator;

import com.netflix.titus.common.model.sanitizer.FieldInvariant;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public class SpELMessageInterpolator implements MessageInterpolator {

    private final ExpressionParser parser = new SpelExpressionParser();
    private final Supplier<EvaluationContext> spelContextFactory;

    public SpELMessageInterpolator(Supplier<EvaluationContext> spelContextFactory) {
        this.spelContextFactory = spelContextFactory;
    }

    @Override
    public String interpolate(String messageTemplate, Context context) {
        Expression expression = parser.parseExpression(messageTemplate, new TemplateParserContext());

        Object effectiveValue = context.getValidatedValue();
        if (context.getConstraintDescriptor().getAnnotation() instanceof FieldInvariant) {
            effectiveValue = new SpELFieldValidator.Root(effectiveValue);
        }
        return (String) expression.getValue(spelContextFactory.get(), effectiveValue);
    }

    @Override
    public String interpolate(String messageTemplate, Context context, Locale locale) {
        return interpolate(messageTemplate, context);
    }
}
