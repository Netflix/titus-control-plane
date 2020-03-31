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

package com.netflix.titus.common.aws;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.StringExt;

public enum AwsInstanceType {

    /*
     * M3 family
     */

    M3_LARGE(AwsInstanceDescriptor.newBuilder("m3.large")
            .cpu(2)
            .memoryGB(7)
            .storageGB(32)
            .networkMbs(300)
            .build()
    ),
    M3_XLARGE(AwsInstanceDescriptor.newBuilder("m3.xlarge")
            .cpu(4)
            .memoryGB(15)
            .storageGB(80)
            .networkMbs(500)
            .build()
    ),
    M3_2XLARGE(AwsInstanceDescriptor.newBuilder("m3.2xlarge")
            .cpu(8)
            .memoryGB(30)
            .storageGB(160)
            .networkMbs(700)
            .build()
    ),

    /*
     * M4 family
     */

    // TODO storage is assumed attached storage, network bandwidth is a guess
    M4_Large(AwsInstanceDescriptor.newBuilder("m4.large")
            .cpu(2)
            .memoryGB(8)
            .storageGB(128)
            .networkMbs(500)
            .ebsOnly()
            .ebsBandwidthMbs(450)
            .build()
    ),
    M4_XLarge(AwsInstanceDescriptor.newBuilder("m4.xlarge")
            .cpu(4)
            .memoryGB(16)
            .storageGB(256)
            .networkMbs(1000)
            .ebsOnly()
            .ebsBandwidthMbs(750)
            .build()
    ),
    M4_2XLarge(AwsInstanceDescriptor.newBuilder("m4.2xlarge")
            .cpu(8)
            .memoryGB(32)
            .storageGB(512)
            .networkMbs(2000)
            .ebsOnly()
            .ebsBandwidthMbs(1000)
            .build()
    ),
    M4_4XLarge(AwsInstanceDescriptor.newBuilder("m4.4xlarge")
            .cpu(16)
            .memoryGB(64)
            .storageGB(512)
            .networkMbs(2000)
            .ebsOnly()
            .ebsBandwidthMbs(2000)
            .build()
    ),
    M4_10XLarge(AwsInstanceDescriptor.newBuilder("m4.10xlarge")
            .cpu(40)
            .memoryGB(160)
            .storageGB(1024)
            .networkMbs(10000)
            .ebsOnly()
            .ebsBandwidthMbs(4000)
            .build()
    ),
    M4_16XLarge(AwsInstanceDescriptor.newBuilder("m4.16xlarge")
            .cpu(64)
            .memoryGB(256)
            .storageGB(1_000)
            .networkMbs(25000)
            .ebsOnly()
            .ebsBandwidthMbs(10_000)
            .build()
    ),

    /*
     * M5 family
     */

    M5_Metal(AwsInstanceDescriptor.newBuilder("m5.metal")
            .cpu(96)
            .memoryGB(384)
            .storageGB(1_024)
            .networkMbs(25_000)
            .ebsOnly()
            .ebsBandwidthMbs(14_000)
            .build()
    ),

    /*
     * R3 family
     */

    // TODO network bandwidth is a guess
    R3_2XLarge(AwsInstanceDescriptor.newBuilder("r3.2xlarge")
            .cpu(8)
            .memoryGB(61)
            .networkMbs(1000)
            .storageGB(160)
            .build()
    ),
    R3_4XLarge(AwsInstanceDescriptor.newBuilder("r3.4xlarge")
            .cpu(16)
            .memoryGB(122)
            .networkMbs(2000)
            .storageGB(320)
            .build()
    ),
    R3_8XLarge(AwsInstanceDescriptor.newBuilder("r3.8xlarge")
            .cpu(32)
            .memoryGB(244)
            .networkMbs(10000)
            .storageGB(640)
            .build()
    ),

    /*
     * R4 family
     */

    R4_2XLarge(AwsInstanceDescriptor.newBuilder("r4.2xlarge")
            .cpu(8)
            .memoryGB(61)
            .networkMbs(10_000)
            .storageGB(160)
            .build()
    ),
    R4_4XLarge(AwsInstanceDescriptor.newBuilder("r4.4xlarge")
            .cpu(16)
            .memoryGB(122)
            .networkMbs(10_000)
            .storageGB(320)
            .build()
    ),
    R4_8XLarge(AwsInstanceDescriptor.newBuilder("r4.8xlarge")
            .cpu(32)
            .memoryGB(244)
            .networkMbs(10_000)
            .storageGB(640)
            .build()
    ),
    R4_16XLarge(AwsInstanceDescriptor.newBuilder("r4.16xlarge")
            .cpu(64)
            .memoryGB(488)
            .networkMbs(25_000)
            .storageGB(1_280)
            .build()
    ),

    /*
     * R5 family
     */
    R5_Metal(AwsInstanceDescriptor.newBuilder("r5.metal")
            .cpu(96)
            .memoryGB(768)
            .storageGB(1_500)
            .networkMbs(25_000)
            .ebsOnly()
            .ebsBandwidthMbs(14_000)
            .build()
    ),

    /*
     * G2 family
     */

    G2_2XLarge(AwsInstanceDescriptor.newBuilder("g2.2xlarge")
            .cpu(8)
            .gpu(1)
            .memoryGB(15)
            .networkMbs(1000)
            .storageGB(60)
            .build()
    ),
    G2_8XLarge(AwsInstanceDescriptor.newBuilder("g2.8xlarge")
            .cpu(32)
            .gpu(4)
            .memoryGB(60)
            .networkMbs(10000)
            .storageGB(240)
            .build()
    ),

    /*
     * G4 family
     */

    G4DN_XLarge(AwsInstanceDescriptor.newBuilder("g4dn.xlarge")
            .cpu(4)
            .gpu(1)
            .memoryGB(16)
            .networkMbs(25000)
            .storageGB(125)
            .build()
    ),
    G4DN_2XLarge(AwsInstanceDescriptor.newBuilder("g4dn.2xlarge")
            .cpu(8)
            .gpu(1)
            .memoryGB(32)
            .networkMbs(25000)
            .storageGB(225)
            .build()
    ),
    G4DN_4XLarge(AwsInstanceDescriptor.newBuilder("g4dn.4xlarge")
            .cpu(16)
            .gpu(1)
            .memoryGB(64)
            .networkMbs(25000)
            .storageGB(225)
            .build()
    ),
    G4DN_8XLarge(AwsInstanceDescriptor.newBuilder("g4dn.8xlarge")
            .cpu(32)
            .gpu(1)
            .memoryGB(128)
            .networkMbs(50000)
            .storageGB(900)
            .build()
    ),
    G4DN_12XLarge(AwsInstanceDescriptor.newBuilder("g4dn.12xlarge")
            .cpu(48)
            .gpu(4)
            .memoryGB(192)
            .networkMbs(50000)
            .storageGB(900)
            .build()
    ),
    G4DN_16XLarge(AwsInstanceDescriptor.newBuilder("g4dn.16xlarge")
            .cpu(64)
            .gpu(1)
            .memoryGB(256)
            .networkMbs(50000)
            .storageGB(900)
            .build()
    ),
    G4DN_Metal(AwsInstanceDescriptor.newBuilder("g4dn.metal")
            .cpu(96)
            .gpu(8)
            .memoryGB(384)
            .networkMbs(100000)
            .storageGB(1800)
            .build()
    ),

    /*
     * P2 family
     */

    P2_XLarge(AwsInstanceDescriptor.newBuilder("p2.xlarge")
            .cpu(4)
            .gpu(1)
            .memoryGB(61)
            .networkMbs(1000)
            .storageGB(60)
            .build()
    ),
    P2_8XLarge(AwsInstanceDescriptor.newBuilder("p2.8xlarge")
            .cpu(32)
            .gpu(8)
            .memoryGB(488)
            .networkMbs(10_000)
            .storageGB(240)
            .build()
    ),
    P2_16XLarge(AwsInstanceDescriptor.newBuilder("p2.16xlarge")
            .cpu(64)
            .gpu(16)
            .memoryGB(732)
            .networkMbs(25_000)
            .storageGB(480)
            .build()
    ),

    /*
     * P3 family
     */


    P3_2XLarge(AwsInstanceDescriptor.newBuilder("p3.2xlarge")
            .cpu(8)
            .gpu(1)
            .memoryGB(61)
            .networkMbs(2_000)
            .storageGB(125)
            .build()
    ),
    P3_8XLarge(AwsInstanceDescriptor.newBuilder("p3.8xlarge")
            .cpu(32)
            .gpu(4)
            .memoryGB(244)
            .networkMbs(10_000)
            .storageGB(500)
            .build()
    ),
    P3_16XLarge(AwsInstanceDescriptor.newBuilder("p3.16xlarge")
            .cpu(64)
            .gpu(8)
            .memoryGB(488)
            .networkMbs(25_000)
            .storageGB(500)
            .build()
    );

    public static final String M4_LARGE_ID = "m4.large";
    public static final String M4_XLARGE_ID = "m4.xlarge";
    public static final String M4_2XLARGE_ID = "m4.2xlarge";
    public static final String M4_4XLARGE_ID = "m4.4xlarge";
    public static final String M4_10XLARGE_ID = "m4.10xlarge";
    public static final String M4_16XLARGE_ID = "m4.16xlarge";

    public static final String R3_2XLARGE_ID = "r3.2xlarge";
    public static final String R3_4XLARGE_ID = "r3.4xlarge";
    public static final String R3_8XLARGE_ID = "r3.8xlarge";

    public static final String R4_2XLARGE_ID = "r4.2xlarge";
    public static final String R4_4XLARGE_ID = "r4.4xlarge";
    public static final String R4_8XLARGE_ID = "r4.8xlarge";

    public static final String G2_XLARGE_ID = "g2.xlarge";
    public static final String G2_2XLARGE_ID = "g2.2xlarge";
    public static final String G2_8XLARGE_ID = "g2.8xlarge";

    public static final String G4DN_XLARGE_ID = "g4dn.xlarge";
    public static final String G4DN_2XLARGE_ID = "g4dn.2xlarge";
    public static final String G4DN_4XLARGE_ID = "g4dn.4xlarge";
    public static final String G4DN_8XLARGE_ID = "g4dn.8xlarge";
    public static final String G4DN_12XLARGE_ID = "g4dn.12xlarge";
    public static final String G4DN_16XLARGE_ID = "g4dn.16xlarge";
    public static final String G4DN_METAL_ID = "g4dn.metal";

    public static final String P2_XLARGE_ID = "p2.xlarge";
    public static final String P2_2XLARGE_ID = "p2.2xlarge";
    public static final String P2_8XLARGE_ID = "p2.8xlarge";

    public static final String P3_2XLARGE_ID = "p3.2xlarge";
    public static final String P3_8XLARGE_ID = "p3.8xlarge";
    public static final String P3_16XLARGE_ID = "p3.16xlarge";

    private static final Map<String, AwsInstanceType> INSTANCES_BY_MODEL = createInstanceByModelMap();

    private final AwsInstanceDescriptor descriptor;

    AwsInstanceType(AwsInstanceDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    public AwsInstanceDescriptor getDescriptor() {
        return descriptor;
    }

    /**
     * Returns {@link AwsInstanceType} given name in the standard format (for example 'm4.xlarge') or as enum ('M4_XLarge').
     */
    public static AwsInstanceType withName(String instanceName) {
        Preconditions.checkArgument(StringExt.isNotEmpty(instanceName), "non empty string expected with AWS instance type name");

        AwsInstanceType type = INSTANCES_BY_MODEL.get(instanceName);
        if (type != null) {
            return type;
        }

        return AwsInstanceType.valueOf(instanceName);
    }

    private static Map<String, AwsInstanceType> createInstanceByModelMap() {
        Map<String, AwsInstanceType> result = new HashMap<>();
        for (AwsInstanceType type : values()) {
            result.put(type.getDescriptor().getId(), type);
        }
        return Collections.unmodifiableMap(result);
    }
}
