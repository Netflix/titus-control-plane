/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.environment;

import java.util.Collections;
import java.util.Map;

import com.netflix.archaius.api.Config;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.environment.internal.MyArchaiusEnvironment;
import com.netflix.titus.common.environment.internal.MyCachingEnvironment;
import com.netflix.titus.common.environment.internal.MyEmptyEnvironment;
import com.netflix.titus.common.environment.internal.MySpringEnvironment;
import com.netflix.titus.common.environment.internal.MyStaticEnvironment;
import com.netflix.titus.common.environment.internal.MyStaticMutableEnvironment;
import com.netflix.titus.common.util.closeable.CloseableReference;
import org.springframework.core.env.Environment;

public final class MyEnvironments {

    private MyEnvironments() {
    }

    public static MyMutableEnvironment newMutable() {
        return new MyStaticMutableEnvironment(Collections.emptyMap());
    }

    public static MyEnvironment newFromMap(Map<String, String> properties) {
        return new MyStaticEnvironment(properties);
    }

    public static MyMutableEnvironment newMutableFromMap(Map<String, String> properties) {
        return new MyStaticMutableEnvironment(properties);
    }

    public static MyEnvironment newSpring(Environment environment) {
        return new MySpringEnvironment(environment);
    }

    public static MyEnvironment newArchaius(Config config) {
        return new MyArchaiusEnvironment(config);
    }

    public static CloseableReference<MyEnvironment> newSpringCaching(Environment environment, long refreshCycleMs, Registry registry) {
        MyCachingEnvironment cached = new MyCachingEnvironment(
                "spring",
                new MySpringEnvironment(environment),
                refreshCycleMs,
                registry
        );
        return CloseableReference.referenceOf(cached, c -> cached.close());
    }

    public static MyEnvironment empty() {
        return MyEmptyEnvironment.getInstance();
    }
}
