/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.framework.reconciler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * {@link ModelAction} wrapper with context information.
 */
public class ModelActionHolder {

    public enum Model {Reference, Running, Store}

    private final Model model;
    private final ModelAction action;

    private ModelActionHolder(Model model, ModelAction action) {
        this.model = model;
        this.action = action;
    }

    public Model getModel() {
        return model;
    }

    public ModelAction getAction() {
        return action;
    }

    public static ModelActionHolder reference(ModelAction action) {
        return new ModelActionHolder(Model.Reference, action);
    }

    public static List<ModelActionHolder> referenceList(ModelAction action) {
        return Collections.singletonList(new ModelActionHolder(Model.Reference, action));
    }

    public static ModelActionHolder running(ModelAction action) {
        return new ModelActionHolder(Model.Running, action);
    }

    public static List<ModelActionHolder> runningList(ModelAction action) {
        return Collections.singletonList(new ModelActionHolder(Model.Running, action));
    }

    public static ModelActionHolder store(ModelAction action) {
        return new ModelActionHolder(Model.Store, action);
    }

    public static List<ModelActionHolder> storeList(ModelAction action) {
        return Collections.singletonList(new ModelActionHolder(Model.Store, action));
    }

    public static List<ModelActionHolder> allModels(ModelAction action) {
        return Arrays.asList(reference(action), running(action), store(action));
    }

    public static List<ModelActionHolder> referenceAndRunning(ModelAction action) {
        return Arrays.asList(reference(action), running(action));
    }

    public static List<ModelActionHolder> referenceAndStore(ModelAction action) {
        return Arrays.asList(reference(action), store(action));
    }

    public static List<ModelActionHolder> runningAndStore(ModelAction action) {
        return Arrays.asList(running(action), store(action));
    }
}
