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

package com.netflix.titus.api.model.event;

public class FailedScaleUpEvent extends ScaleUpEvent {

    private final String errorMessage;

    public FailedScaleUpEvent(String instanceGroupId, int currentSize, int requestedSize, String errorMessage) {
        super(instanceGroupId, currentSize, requestedSize);
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        FailedScaleUpEvent that = (FailedScaleUpEvent) o;

        return errorMessage != null ? errorMessage.equals(that.errorMessage) : that.errorMessage == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (errorMessage != null ? errorMessage.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FailedScaleUpEvent{" +
                "instanceGroupId='" + getInstanceGroupId() + '\'' +
                ", currentSize=" + getCurrentSize() +
                ", requestedSize=" + getRequestedSize() +
                ", errorMessage=" + errorMessage +
                '}';
    }
}
