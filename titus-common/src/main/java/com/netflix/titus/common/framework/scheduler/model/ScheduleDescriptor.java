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

package com.netflix.titus.common.framework.scheduler.model;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.retry.Retryer;
import com.netflix.titus.common.util.retry.Retryers;

public class ScheduleDescriptor {

    private final String name;
    private final String description;
    private final Duration interval;
    private final Supplier<Retryer> retryerSupplier;
    private final Duration timeout;
    private final Consumer<ScheduledAction> onSuccessHandler;
    private final BiConsumer<ScheduledAction, Throwable> onErrorHandler;

    private ScheduleDescriptor(String name,
                               String description,
                               Duration interval,
                               Duration timeout,
                               Supplier<Retryer> retryerSupplier,
                               Consumer<ScheduledAction> onSuccessHandler,
                               BiConsumer<ScheduledAction, Throwable> onErrorHandler) {
        this.name = name;
        this.description = description;
        this.interval = interval;
        this.retryerSupplier = retryerSupplier;
        this.timeout = timeout;
        this.onSuccessHandler = onSuccessHandler;
        this.onErrorHandler = onErrorHandler;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Supplier<Retryer> getRetryerSupplier() {
        return retryerSupplier;
    }

    public Duration getInterval() {
        return interval;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public Consumer<ScheduledAction> getOnSuccessHandler() {
        return onSuccessHandler;
    }

    public BiConsumer<ScheduledAction, Throwable> getOnErrorHandler() {
        return onErrorHandler;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScheduleDescriptor that = (ScheduleDescriptor) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(description, that.description) &&
                Objects.equals(interval, that.interval) &&
                Objects.equals(retryerSupplier, that.retryerSupplier) &&
                Objects.equals(timeout, that.timeout) &&
                Objects.equals(onSuccessHandler, that.onSuccessHandler) &&
                Objects.equals(onErrorHandler, that.onErrorHandler);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, interval, retryerSupplier, timeout, onSuccessHandler, onErrorHandler);
    }

    @Override
    public String toString() {
        return "ScheduleDescriptor{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", interval=" + interval +
                ", retryerSupplier=" + retryerSupplier +
                ", timeout=" + timeout +
                ", onSuccessHandler=" + onSuccessHandler +
                ", onErrorHandler=" + onErrorHandler +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String name;
        private String description;
        private Duration interval;
        private Duration timeout;

        private Supplier<Retryer> retryerSupplier = () -> Retryers.exponentialBackoff(1, 5, TimeUnit.SECONDS);

        private Consumer<ScheduledAction> onSuccessHandler = schedule -> {
        };

        private BiConsumer<ScheduledAction, Throwable> onErrorHandler = (schedule, error) -> {
        };

        private Builder() {
        }

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder withInterval(Duration interval) {
            this.interval = interval;
            return this;
        }

        public Builder withTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withRetryerSupplier(Supplier<Retryer> retryerSupplier) {
            this.retryerSupplier = retryerSupplier;
            return this;
        }

        public Builder withOnErrorHandler(BiConsumer<ScheduledAction, Throwable> onErrorHandler) {
            this.onErrorHandler = onErrorHandler;
            return this;
        }

        public Builder withOnSuccessHandler(Consumer<ScheduledAction> onSuccessHandler) {
            this.onSuccessHandler = onSuccessHandler;
            return this;
        }

        public ScheduleDescriptor build() {
            Preconditions.checkNotNull(name, "name cannot be null");
            Preconditions.checkNotNull(description, "description cannot be null");
            Preconditions.checkNotNull(interval, "interval cannot be null");
            Preconditions.checkNotNull(timeout, "timeout cannot be null");
            Preconditions.checkNotNull(retryerSupplier, "retryerSupplier cannot be null");
            Preconditions.checkNotNull(onSuccessHandler, "onSuccessHandler cannot be null");
            Preconditions.checkNotNull(onErrorHandler, "onErrorHandler cannot be null");

            return new ScheduleDescriptor(name, description, interval, timeout, retryerSupplier, onSuccessHandler, onErrorHandler);
        }
    }
}
