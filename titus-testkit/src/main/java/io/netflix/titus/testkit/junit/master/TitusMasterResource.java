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

package io.netflix.titus.testkit.junit.master;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import io.netflix.titus.testkit.embedded.EmbeddedTitusOperations;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import org.junit.rules.ExternalResource;

/**
 * Run TitusMaster server with mocked external integrations (mesos, storage).
 */
public class TitusMasterResource extends ExternalResource {

    private final EmbeddedTitusMaster embeddedTitusMaster;

    public TitusMasterResource(EmbeddedTitusMaster embeddedTitusMaster) {
        this.embeddedTitusMaster = embeddedTitusMaster;
    }

    @Override
    public void before() throws Throwable {
        embeddedTitusMaster.boot();
    }

    @Override
    public void after() {
        Stopwatch timer = Stopwatch.createStarted();
        embeddedTitusMaster.shutdown();
        System.out.println("Execution time: " + timer.elapsed(TimeUnit.MILLISECONDS));
    }

    public EmbeddedTitusMaster getMaster() {
        return embeddedTitusMaster;
    }

    public EmbeddedTitusOperations getOperations() {
        return new EmbeddedTitusOperations(embeddedTitusMaster);
    }
}
