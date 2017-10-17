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

package io.netflix.titus.api.model.event;

public class ScaleUpEvent extends AutoScaleEvent {

    private final float currentSize;
    private final int requestedSize;

    public ScaleUpEvent(String instanceGroupId, int currentSize, int requestedSize) {
        super(instanceGroupId);
        this.currentSize = currentSize;
        this.requestedSize = requestedSize;
    }

    public float getCurrentSize() {
        return currentSize;
    }

    public int getRequestedSize() {
        return requestedSize;
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

        ScaleUpEvent that = (ScaleUpEvent) o;

        if (Float.compare(that.currentSize, currentSize) != 0) {
            return false;
        }
        return requestedSize == that.requestedSize;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (currentSize != +0.0f ? Float.floatToIntBits(currentSize) : 0);
        result = 31 * result + requestedSize;
        return result;
    }

    @Override
    public String toString() {
        return "ScaleUpEvent{" +
                "instanceGroupId='" + getInstanceGroupId() +
                "currentSize=" + currentSize +
                ", requestedSize=" + requestedSize +
                "}";
    }
}
