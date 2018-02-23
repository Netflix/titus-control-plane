package io.netflix.titus.common.framework.fit.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.framework.fit.FitComponent;
import io.netflix.titus.common.framework.fit.FitInjection;

public class DefaultFitComponent implements FitComponent {

    private final String id;

    private final ConcurrentMap<String, FitComponent> children = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, FitInjection> injections = new ConcurrentHashMap<>();

    public DefaultFitComponent(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void addChild(FitComponent childComponent) {
        children.put(childComponent.getId(), childComponent);
    }

    @Override
    public List<FitComponent> getChildren() {
        return new ArrayList<>(children.values());
    }

    @Override
    public FitComponent getChild(String id) {
        return Preconditions.checkNotNull(children.get(id), "FitComponent %s not found", id);
    }

    @Override
    public Optional<FitComponent> findChild(String id) {
        return Optional.ofNullable(children.get(id));
    }

    @Override
    public FitComponent addInjection(FitInjection injection) {
        injections.put(injection.getId(), injection);
        return this;
    }

    @Override
    public List<FitInjection> getInjections() {
        return new ArrayList<>(injections.values());
    }

    @Override
    public FitInjection getInjection(String id) {
        return Preconditions.checkNotNull(injections.get(id), "Injection %s not found", id);
    }

    @Override
    public Optional<FitInjection> findInjection(String id) {
        return Optional.ofNullable(injections.get(id));
    }

    @Override
    public void visitInjections(Consumer<FitInjection> evaluator) {
        injections.values().forEach(evaluator);
        children.values().forEach(c -> c.visitInjections(evaluator));
    }
}
