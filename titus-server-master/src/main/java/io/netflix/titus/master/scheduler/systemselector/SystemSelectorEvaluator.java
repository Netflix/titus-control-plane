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

package io.netflix.titus.master.scheduler.systemselector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import io.netflix.titus.api.scheduler.service.SchedulerException;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.cache.Cache;
import io.netflix.titus.common.util.cache.Caches;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.master.MetricConstants.METRIC_SCHEDULING_SERVICE;

/**
 * Evaluates system selector expressions based on an expression statement and the context.
 */
@Singleton
public class SystemSelectorEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(SystemSelectorEvaluator.class);
    private static final String SCRIPT_ENGINE_NAME = "nashorn";
    private static final String METRIC_SCRIPT_ENGINE_EVALUATORS = "systemSelectorEvaluator.scriptEngineEvaluators";
    private final Cache<Long, ScriptEngineEvaluator> scriptEngineEvaluators;
    private final ScriptEngine validatorScriptEngine;

    @Inject
    public SystemSelectorEvaluator(SchedulerConfiguration configuration,TitusRuntime titusRuntime) {
        scriptEngineEvaluators = Caches.instrumentedCacheWithMaxSize(
                configuration.getSchedulerMaxConcurrent(), METRIC_SCHEDULING_SERVICE + METRIC_SCRIPT_ENGINE_EVALUATORS, titusRuntime.getRegistry()
        );
        validatorScriptEngine = new ScriptEngineManager().getEngineByName(SCRIPT_ENGINE_NAME);
    }

    public void validate(String expression, Map<String, Object> context) {
        //TODO put this code back once we add have empty instances for all context objects
//        try {
//            ScriptContext scriptContext = validatorScriptEngine.getContext();
//            for (Map.Entry<String, Object> entry : context.entrySet()) {
//                scriptContext.setAttribute(entry.getKey(), entry.getValue(), ScriptContext.ENGINE_SCOPE);
//            }
//            validatorScriptEngine.eval(expression, scriptContext);
//        } catch (ScriptException e) {
//            if (logger.isDebugEnabled()) {
//                logger.debug("Unable to eval expression: {}", expression, e);
//            }
//            throw SchedulerException.systemSelectorEvaluationError("Unable to evaluate expression: %s", e, expression);
//        }
    }

    public boolean evaluate(String expression, Map<String, Object> context) {
        // Create a script engine per scheduler thread in order to reduce contention
        long threadId = Thread.currentThread().getId();
        ScriptEngineEvaluator scriptEngineEvaluator = scriptEngineEvaluators.get(threadId, t -> new ScriptEngineEvaluator());
        if (scriptEngineEvaluator == null) {
            throw SchedulerException.systemSelectorEvaluationError("Unable to evaluate expression: %s because scriptEngineEvaluator was null", null, expression);
        }
        return scriptEngineEvaluator.evaluate(expression, context);
    }

    private class ScriptEngineEvaluator {

        private final ScriptEngine scriptEngine;
        private final Compilable compilable;

        private final ConcurrentMap<String, CompiledScript> compiledScripts = new ConcurrentHashMap<>();

        private ScriptEngineEvaluator() {
            scriptEngine = new ScriptEngineManager().getEngineByName(SCRIPT_ENGINE_NAME);
            compilable = (Compilable) scriptEngine;
        }

        private boolean evaluate(String expression, Map<String, Object> context) {
            CompiledScript compiledScript = getOrCreateCompileScript(expression);
            if (compiledScript != null) {
                ScriptContext scriptContext = scriptEngine.getContext();
                for (Map.Entry<String, Object> entry : context.entrySet()) {
                    scriptContext.setAttribute(entry.getKey(), entry.getValue(), ScriptContext.ENGINE_SCOPE);
                }
                try {
                    logger.debug("Evaluating expression: {}", expression);
                    Object result = scriptEngine.eval(expression, scriptContext);
                    logger.debug("Evaluated expression: {} and got result: {}", expression, result);
                    if (result == Boolean.TRUE) {
                        return true;
                    }

                } catch (ScriptException e) {
                    logger.debug("Unable to evaluate expression: {}", expression, e);
                    throw SchedulerException.systemSelectorEvaluationError("Unable to evaluate expression: %s", e, expression);
                }
            }
            return false;
        }

        private CompiledScript getOrCreateCompileScript(String expression) {
            return compiledScripts.computeIfAbsent(expression, k -> {
                try {
                    return compilable.compile(expression);
                } catch (ScriptException e) {
                    logger.debug("Unable to compile expression: {}", expression, e);
                }
                return null;
            });
        }
    }
}
