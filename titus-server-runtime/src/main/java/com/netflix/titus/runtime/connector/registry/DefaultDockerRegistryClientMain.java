/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.registry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

public class DefaultDockerRegistryClientMain {

    static {
        // Uncomment to enable logging for troubleshooting
//        ConsoleAppender consoleAppender = new ConsoleAppender();
//        consoleAppender.setName("console");
//        consoleAppender.setThreshold(Level.DEBUG);
//        consoleAppender.setLayout(new PatternLayout("%d %-5p %X{requestId} %C:%L [%t] [%M] %m%n"));
//        consoleAppender.setWriter(new OutputStreamWriter(System.out));
//
//        LogManager.getRootLogger().addAppender(consoleAppender);
    }

    private static final int CONCURRENCY = 100;
    private static final long INTERVAL_MS = 70_000;

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: DefaultDockerRegistryClientMain <registry_uri> <image_name> <image_tag>");
            System.exit(-1);
        }

        String registryUri = args[0];
        String imageName = args[1];
        String imageTag = args[2];

        DefaultDockerRegistryClient client = newClient(registryUri);

        while (true) {
            resolve(imageName, imageTag, client);
            try {
                Thread.sleep(INTERVAL_MS);
            } catch (InterruptedException ignore) {
            }
        }
    }

    private static void resolve(String imageName, String imageTag, DefaultDockerRegistryClient client) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            List<String> results = Flux.merge(Evaluators.evaluateTimes(CONCURRENCY, n -> newResolveAction(imageName, imageTag, client)))
                    .collectList()
                    .block(Duration.ofSeconds(30));
            System.out.println(String.format("Next round completed in %sms:", stopwatch.elapsed(TimeUnit.MILLISECONDS)));
            results.forEach(r -> System.out.println("    " + r));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Flux<String> newResolveAction(String imageName, String imageTag, DefaultDockerRegistryClient client) {
        return client.getImageDigest(imageName, imageTag)
                .materialize()
                .map(signal ->
                        signal.getType() == SignalType.ON_NEXT
                                ? "Result=" + signal.get()
                                : "Error=" + signal.getThrowable().getCause().getMessage())
                .flux();
    }

    private static DefaultDockerRegistryClient newClient(String registryUri) {
        return new DefaultDockerRegistryClient(
                new TitusRegistryClientConfiguration() {
                    @Override
                    public String getRegistryUri() {
                        return registryUri;
                    }

                    @Override
                    public boolean isSecure() {
                        return true;
                    }

                    @Override
                    public int getRegistryTimeoutMs() {
                        return 30_000;
                    }

                    @Override
                    public int getRegistryRetryCount() {
                        return 3;
                    }

                    @Override
                    public int getRegistryRetryDelayMs() {
                        return 5_000;
                    }
                },
                TitusRuntimes.internal()
        );
    }
}
