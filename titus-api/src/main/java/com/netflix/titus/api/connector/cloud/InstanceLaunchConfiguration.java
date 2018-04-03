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

package com.netflix.titus.api.connector.cloud;

public class InstanceLaunchConfiguration {

    private final String id;
    private final String instanceType;

    public InstanceLaunchConfiguration(String id, String instanceType) {
        this.id = id;
        this.instanceType = instanceType;
    }

    public String getId() {
        return id;
    }

    public String getInstanceType() {
        return instanceType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InstanceLaunchConfiguration that = (InstanceLaunchConfiguration) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        return instanceType != null ? instanceType.equals(that.instanceType) : that.instanceType == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (instanceType != null ? instanceType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "InstanceLaunchConfiguration{" +
                "id='" + id + '\'' +
                ", instanceType='" + instanceType + '\'' +
                '}';
    }
}
