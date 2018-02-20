package io.netflix.titus.common.framework.fit;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface FitComponent {

    String getId();

    void addChild(FitComponent childComponent);

    List<FitComponent> getChildren();

    FitComponent getChild(String id);

    Optional<FitComponent> findChild(String id);

    FitComponent addInjection(FitInjection fitInjection);

    List<FitInjection> getInjections();

    FitInjection getInjection(String id);

    Optional<FitInjection> findInjection(String name);

    void visitInjections(Consumer<FitInjection> evaluator);
}
