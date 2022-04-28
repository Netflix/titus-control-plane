/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.master.service.management.kube.crd;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public class Fabric8IOModelConverters {

    /**
     * TODO Get rid of it once all capacity group names follow the Kubernetes naming convention.
     */
    private static final String LABEL_ORIGINAL_NAME = "capacitygroup.com.netflix.titus/original-name";

    public static ApplicationSLA toApplicationSLA(F8IOCapacityGroup f8IOCapacityGroup) {
        String name = toApplicationSlaName(f8IOCapacityGroup);
        String tierAnnotation = f8IOCapacityGroup.getMetadata().getAnnotations().get("tier");
        Tier tier = "Critical".equalsIgnoreCase(tierAnnotation) ? Tier.Critical : Tier.Flex;

        return ApplicationSLA.newBuilder()
                .withAppName(name)
                .withResourcePool(f8IOCapacityGroup.getSpec().getResourcePoolName())
                .withInstanceCount(f8IOCapacityGroup.getSpec().getInstanceCount())
                .withResourceDimension(toCoreResourceDimensions(f8IOCapacityGroup.getSpec().getResourceDimensions()))
                .withSchedulerName(f8IOCapacityGroup.getSpec().getSchedulerName())
                .withTier(tier)
                .build();
    }

    public static ObjectMeta toF8IOMetadata(ApplicationSLA applicationSLA) {
        String original = applicationSLA.getAppName();
        String name = "default".equalsIgnoreCase(original) ? "default" : toValidKubeCrdName(original);

        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName(name);
        objectMeta.setAnnotations(CollectionsExt.asMap(
                "tier", applicationSLA.getTier().name(),
                LABEL_ORIGINAL_NAME, original
        ));
        return objectMeta;
    }

    public static String toApplicationSlaName(F8IOCapacityGroup f8IOCapacityGroup) {
        String name = f8IOCapacityGroup.getMetadata().getName();
        if ("default".equalsIgnoreCase(name)) {
            name = "DEFAULT";
        } else {
            String original = f8IOCapacityGroup.getMetadata().getAnnotations().get(LABEL_ORIGINAL_NAME);
            if (!StringExt.isEmpty(original)) {
                name = original;
            }
        }
        return name;
    }

    public static String toValidKubeCrdName(String originalName) {
        return originalName.replaceAll("_", "-").toLowerCase();
    }

    public static F8IOCapacityGroupSpec toF8IOCapacityGroupSpec(ApplicationSLA applicationSLA) {
        F8IOCapacityGroupSpec spec = new F8IOCapacityGroupSpec();
        spec.setCapacityGroupName(applicationSLA.getAppName());
        spec.setResourcePoolName(applicationSLA.getResourcePool());
        spec.setCreatedBy("capacity-group-reconciler");
        spec.setInstanceCount(applicationSLA.getInstanceCount());
        spec.setSchedulerName(applicationSLA.getSchedulerName());
        spec.setResourceDimensions(toF8IOResourceDimension(applicationSLA.getResourceDimension()));
        return spec;
    }

    private static ResourceDimension toCoreResourceDimensions(F8IOResourceDimension resourceDimensions) {
        return ResourceDimension.newBuilder()
                .withCpus(resourceDimensions.getCpu())
                .withGpu(resourceDimensions.getGpu())
                .withMemoryMB(resourceDimensions.getMemoryMB())
                .withDiskMB(resourceDimensions.getDiskMB())
                .withNetworkMbs(resourceDimensions.getNetworkMBPS())
                .build();
    }

    private static F8IOResourceDimension toF8IOResourceDimension(ResourceDimension resourceDimension) {
        F8IOResourceDimension f8IOResourceDimension = new F8IOResourceDimension();
        f8IOResourceDimension.setCpu((int) resourceDimension.getCpu());
        f8IOResourceDimension.setGpu((int) resourceDimension.getGpu());
        f8IOResourceDimension.setMemoryMB((int) resourceDimension.getMemoryMB());
        f8IOResourceDimension.setDiskMB((int) resourceDimension.getDiskMB());
        f8IOResourceDimension.setNetworkMBPS((int) resourceDimension.getNetworkMbs());
        return f8IOResourceDimension;
    }
}
