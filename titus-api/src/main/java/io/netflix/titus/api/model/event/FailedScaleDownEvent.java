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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FailedScaleDownEvent extends ScaleDownEvent {

    private final Map<String, String> failures;
    private final String errorMessage;

    public FailedScaleDownEvent(String instanceGroupId, Set<String> terminatedInstanceIds, Map<String, String> failures) {
        super(instanceGroupId, terminatedInstanceIds);
        this.failures = failures;
        this.errorMessage = "";
    }

    public FailedScaleDownEvent(String instanceGroupId, String errorMessage) {
        super(instanceGroupId, Collections.emptySet());
        this.errorMessage = errorMessage;
        this.failures = Collections.emptyMap();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Map<String, String> getFailures() {
        return failures;
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

        FailedScaleDownEvent that = (FailedScaleDownEvent) o;

        if (failures != null ? !failures.equals(that.failures) : that.failures != null) {
            return false;
        }
        return errorMessage != null ? errorMessage.equals(that.errorMessage) : that.errorMessage == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (failures != null ? failures.hashCode() : 0);
        result = 31 * result + (errorMessage != null ? errorMessage.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "FailedScaleDownEvent{" +
                "instanceGroupId='" + getInstanceGroupId() +
                ", errorMessage=" + errorMessage +
                ", terminatedInstanceIds=" + getTerminatedInstanceIds() +
                ", failures=" + failures +
                "} " + super.toString();
    }
}
