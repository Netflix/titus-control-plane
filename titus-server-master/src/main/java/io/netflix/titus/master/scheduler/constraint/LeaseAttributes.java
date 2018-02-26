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

package io.netflix.titus.master.scheduler.constraint;

import com.netflix.fenzo.VirtualMachineLease;
import io.netflix.titus.common.util.StringExt;
import org.apache.mesos.Protos;

public class LeaseAttributes {
    public static String getOrDefault(VirtualMachineLease lease, String attrName, String defaultValue) {
        if (lease.getAttributeMap() == null) {
            return defaultValue;
        }
        Protos.Attribute attr = lease.getAttributeMap().get(attrName);
        if (attr == null || attr.getText() == null) {
            return defaultValue;
        }
        String attrValue = StringExt.safeTrim(attr.getText().getValue());
        return attrValue.isEmpty() ? defaultValue : attrValue;
    }
}
