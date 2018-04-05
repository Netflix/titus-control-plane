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

package com.netflix.titus.master.scheduler.systemselector;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.scheduler.model.Match;
import com.netflix.titus.api.scheduler.model.Must;
import com.netflix.titus.api.scheduler.model.Should;
import com.netflix.titus.api.scheduler.model.SystemSelector;
import com.netflix.titus.api.scheduler.service.SchedulerException;
import com.netflix.titus.api.scheduler.store.SchedulerStore;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import rx.Completable;
import rx.Observable;

import static com.netflix.titus.common.util.guice.ProxyType.ActiveGuard;
import static com.netflix.titus.common.util.guice.ProxyType.Logging;
import static com.netflix.titus.common.util.guice.ProxyType.Spectator;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

@Singleton
@ProxyConfiguration(types = {Logging, Spectator, ActiveGuard})
public class SystemSelectorService {

    private final ConcurrentMap<String, SystemSelector> systemSelectorsById = new ConcurrentHashMap<>();
    private final Comparator<SystemSelector> priorityComparator = Comparator.comparing(SystemSelector::getPriority)
            .thenComparing(SystemSelector::getId);
    private final SchedulerConfiguration configuration;
    private final SchedulerStore schedulerStore;
    private final SystemSelectorEvaluator systemSelectorEvaluator;

    private volatile List<Match> shouldMatchesForEvaluation = Collections.emptyList();
    private volatile List<Match> mustMatchesForEvaluation = Collections.emptyList();

    @Inject
    public SystemSelectorService(SchedulerConfiguration configuration,
                                 SchedulerStore schedulerStore,
                                 SystemSelectorEvaluator systemSelectorEvaluator) {
        this.configuration = configuration;
        this.schedulerStore = schedulerStore;
        this.systemSelectorEvaluator = systemSelectorEvaluator;
    }

    @Activator
    public void enterActiveMode() {
        try {
            List<SystemSelector> systemSelectors = schedulerStore.retrieveSystemSelectors()
                    .toList()
                    .toBlocking()
                    .first();
            systemSelectors.forEach(s -> systemSelectorsById.put(s.getId(), s));
            updateMatchesForEvaluation();
        } catch (Exception e) {
            throw SchedulerException.systemSelectorInitializationError("Unable to initialize", e);
        }
    }

    public List<SystemSelector> getSystemSelectors() {
        return systemSelectorsById.values().stream().sorted(priorityComparator).collect(toList());
    }

    public SystemSelector getSystemSelector(String id) {
        return SchedulerException.checkSystemSelectorFound(systemSelectorsById.get(id), id);
    }

    public Completable createSystemSelector(SystemSelector systemSelector) {
        return Observable.fromCallable(() -> {
            String id = systemSelector.getId();
            SchedulerException.checkSystemSelectorAlreadyExists(systemSelectorsById.get(id), id);
            validateSystemSelectorExpressions(systemSelector);
            return systemSelector;
        }).toCompletable().andThen(schedulerStore.storeSystemSelector(systemSelector)).andThen(Completable.fromAction(() -> {
            systemSelectorsById.put(systemSelector.getId(), systemSelector);
            updateMatchesForEvaluation();
        }));
    }

    public Completable updateSystemSelector(String id, SystemSelector systemSelector) {
        return Observable.fromCallable(() -> getSystemSelector(id)).toCompletable()
                .andThen(deleteSystemSelector(id))
                .andThen(createSystemSelector(systemSelector));
    }

    public Completable deleteSystemSelector(String id) {
        return Observable.fromCallable(() -> getSystemSelector(id)).toCompletable()
                .andThen(schedulerStore.deleteSystemSelector(id)).andThen(Completable.fromAction(() -> {
                    systemSelectorsById.remove(id);
                    updateMatchesForEvaluation();
                }));
    }

    public List<Match> getShouldMatchesForEvaluation() {
        if (!configuration.isSystemSelectorsEnabled()) {
            return Collections.emptyList();
        }
        return shouldMatchesForEvaluation;
    }

    public List<Match> getMustMatchesForEvaluation() {
        if (!configuration.isSystemSelectorsEnabled()) {
            return Collections.emptyList();
        }
        return mustMatchesForEvaluation;
    }

    private void updateMatchesForEvaluation() {
        Map<Class<?>, List<Match>> selectorListsByType = systemSelectorsById.values().stream()
                .filter(SystemSelector::isEnabled)
                .sorted(priorityComparator)
                .map(this::getMatchByType)
                .collect(groupingBy(Pair::getLeft, mapping(Pair::getRight, toList())));
        shouldMatchesForEvaluation = selectorListsByType.getOrDefault(Should.class, Collections.emptyList());
        mustMatchesForEvaluation = selectorListsByType.getOrDefault(Must.class, Collections.emptyList());
    }

    private void validateSystemSelectorExpressions(SystemSelector systemSelector) {
        Pair<Class<?>, Match> matchByType = getMatchByType(systemSelector);
        Match match = matchByType.getRight();
        if (match != null) {
            //TODO create fake context for validation
            Map<String, Object> context = new HashMap<>();
            systemSelectorEvaluator.validate(match.getSelectExpression(), context);
            systemSelectorEvaluator.validate(match.getMatchExpression(), context);
        }
    }

    private Pair<Class<?>, Match> getMatchByType(SystemSelector systemSelector) {
        if (systemSelector.getShould() != null) {
            return Pair.of(Should.class, (Match) systemSelector.getShould().getOperator());
        } else if (systemSelector.getMust() != null) {
            return Pair.of(Must.class, (Match) systemSelector.getMust().getOperator());
        } else {
            return Pair.of(Object.class, null);
        }
    }
}
