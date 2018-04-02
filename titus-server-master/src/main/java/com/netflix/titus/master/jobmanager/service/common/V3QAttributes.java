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

package com.netflix.titus.master.jobmanager.service.common;

import java.util.Objects;

import com.netflix.fenzo.queues.QAttributes;

public class V3QAttributes implements QAttributes {

    private final int tierNumber;
    private final String bucketName;

    public V3QAttributes(int tierNumber, String bucketName) {
        this.tierNumber = tierNumber;
        this.bucketName = bucketName;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public int getTierNumber() {
        return tierNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        V3QAttributes that = (V3QAttributes) o;
        return tierNumber == that.tierNumber &&
                Objects.equals(bucketName, that.bucketName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tierNumber, bucketName);
    }

    @Override
    public String toString() {
        return "V3QAttributes{" +
                "tierNumber=" + tierNumber +
                ", bucketName='" + bucketName + '\'' +
                '}';
    }
}
