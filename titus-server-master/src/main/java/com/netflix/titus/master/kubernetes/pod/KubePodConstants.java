/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod;

public final class KubePodConstants {
    private KubePodConstants() {
    }

    public static final String DEFAULT_NAMESPACE = "default";
    public static final String DEFAULT_IMAGE_PULL_POLICY = "IfNotPresent";

    public static String NEVER_RESTART_POLICY = "Never";

    public static final String DEFAULT_DNS_POLICY = "Default";

    public static final String JOB_ID = "v3.job.titus.netflix.com/job-id";
    public static final String TASK_ID = "v3.job.titus.netflix.com/task-id";

    // Resources
    public static final String RESOURCE_CPU = "cpu";
    public static final String RESOURCE_MEMORY = "memory";
    public static final String RESOURCE_EPHERMERAL_STORAGE = "ephemeral-storage";
    public static final String RESOURCE_NETWORK = "titus/network";
    public static final String RESOURCE_GPU = "nvidia.com/gpu";

    public static final String INSTANCE_TYPE = "node.titus.netflix.com/itype";
    public static final String REGION = "node.titus.netflix.com/region";
    public static final String STACK = "node.titus.netflix.com/stack";
    public static final String AVAILABILITY_ZONE = "failure-domain.beta.kubernetes.io/zone";

    // Pod Networking
    public static final String EGRESS_BANDWIDTH = "kubernetes.io/egress-bandwidth";
    public static final String INGRESS_BANDWIDTH = "kubernetes.io/ingress-bandwidth";

    // Pod ENI
    public static final String IPV4_ADDRESS = "network.netflix.com/address-ipv4";
    public static final String IPv4_PREFIX_LENGTH = "network.netflix.com/prefixlen-ipv4";
    public static final String IPv6_ADDRESS = "network.netflix.com/address-ipv6";
    public static final String IPv6_PREFIX_LENGTH = "network.netflix.com/prefixlen-ipv6";

    public static final String BRANCH_ENI_ID = "network.netflix.com/branch-eni-id";
    public static final String BRANCH_ENI_MAC = "network.netflix.com/branch-eni-mac";
    public static final String BRANCH_ENI_VPC_ID = "network.netflix.com/branch-eni-vpc";
    public static final String BRANCH_ENI_SUBNET = "network.netflix.com/branch-eni-subnet";

    public static final String TRUNK_ENI_ID = "network.netflix.com/trunk-eni-id";
    public static final String TRUNK_ENI_MAC = "network.netflix.com/trunk-eni-mac";
    public static final String TRUNK_ENI_VPC = "network.netflix.com/trunk-eni-vpc";

    public static final String VLAN_ID = "network.netflix.com/vlan-id";
    public static final String ALLOCATION_INDEX = "network.netflix.com/allocation-idx";

    // Security
    public static final String IAM_ROLE = "iam.amazonaws.com/role";
    public static final String SECURITY_GROUPS_LEGACY = "network.titus.netflix.com/securityGroups";
    // https://kubernetes.io/docs/tutorials/clusters/apparmor/#securing-a-pod
    public static final String PREFIX_APP_ARMOR = "container.apparmor.security.beta.kubernetes.io";

    public static final String POD_SCHEMA_VERSION = "pod.netflix.com/pod-schema-version";
    public static final String POD_SYSTEM_ENV_VAR_NAMES = "pod.titus.netflix.com/system-env-var-names";

    // App-specific fields
    public static final String WORKLOAD_NAME = "workload.netflix.com/name";
    public static final String WORKLOAD_DETAIL = "workload.netflix.com/detail";
    public static final String WORKLOAD_STACK = "workload.netflix.com/stack";
    public static final String WORKLOAD_SEQUENCE = "workload.netflix.com/sequence";
    public static final String WORKLOAD_OWNER_EMAIL = "workload.netflix.com/owner-email";

    // Titus-specific fields
    public static final String JOB_ACCEPTED_TIMESTAMP_MS = "v3.job.titus.netflix.com/accepted-timestamp-ms";
    public static final String JOB_TYPE = "v3.job.titus.netflix.com/type";
    public static final String JOB_DESCRIPTOR = "v3.job.titus.netflix.com/descriptor";
    public static final String ENTRYPOINT_SHELL_SPLITTING_ENABLED = "pod.titus.netflix.com/entrypoint-shell-splitting-enabled";
    public static final String JOB_DISRUPTION_BUDGET_POLICY_NAME = "v3.job.titus.netflix.com/disruption-budget-policy";

    // Networking
    public static final String SUBNETS_LEGACY = "network.titus.netflix.com/subnets";
    public static final String ACCOUNT_ID_LEGACY = "network.titus.netflix.com/accountId";
    public static final String NETWORK_ACCOUNT_ID = "network.netflix.com/account-id";
    public static final String NETWORK_BURSTING_ENABLED = "network.netflix.com/network-bursting-enabled";
    public static final String NETWORK_ASSIGN_IVP6_ADDRESS = "network.netflix.com/assign-ipv6-address";
    public static final String NETWORK_ELASTIC_IP_POOL = "network.netflix.com/elastic-ip-pool";
    public static final String NETWORK_ELASTIC_IPS = "network.netflix.com/elastic-ips";
    public static final String NETWORK_IMDS_REQUIRE_TOKEN = "network.netflix.com/imds-require-token";
    public static final String NETWORK_JUMBO_FRAMES_ENABLED = "network.netflix.com/jumbo-frames-enabled";
    public static final String NETWORK_SECURITY_GROUPS = "network.netflix.com/security-groups";
    public static final String NETWORK_SUBNET_IDS = "network.netflix.com/subnet-ids";
    public static final String NETWORK_STATIC_IP_ALLOCATION_UUID = "network.netflix.com/static-ip-allocation-uuid";
    public static final String NETWORK_EFFECTIVE_NETWORK_MODE = "network.netflix.com/effective-network-mode";
    public static final String NETWORK_IP_ADDRESS = "network.netflix.com/address-ip";
    public static final String NETWORK_IPV4_EIP = "network.netflix.com/address-elastic-ipv4";
    public static final String NETWORK_IPV4_TRANSITION_ADDRESS = "network.netflix.com/address-transition-ipv4";
    // Storage
    public static final String STORAGE_EBS_VOLUME_ID = "ebs.volume.netflix.com/volume-id";
    public static final String STORAGE_EBS_MOUNT_PATH = "ebs.volume.netflix.com/mount-path";
    public static final String STORAGE_EBS_MOUNT_PERM = "ebs.volume.netflix.com/mount-perm";
    public static final String STORAGE_EBS_FS_TYPE = "ebs.volume.netflix.com/fs-type";

    // Security
    public static final String SECURITY_APP_METADATA = "security.netflix.com/workload-metadata";
    public static final String SECURITY_APP_METADATA_SIG = "security.netflix.com/workload-metadata-sig";

    // Pod Features
    public static final String POD_CPU_BURSTING_ENABLED = "pod.netflix.com/cpu-bursting-enabled";
    public static final String POD_KVM_ENABLED = "pod.netflix.com/kvm-enabled";
    public static final String POD_FUSE_ENABLED = "pod.netflix.com/fuse-enabled";
    public static final String POD_HOSTNAME_STYLE = "pod.netflix.com/hostname-style";
    public static final String POD_OOM_SCORE_ADJ = "pod.netflix.com/oom-score-adj";
    public static final String POD_SCHED_POLICY = "pod.netflix.com/sched-policy";
    public static final String POD_SECCOMP_AGENT_NET_ENABLED = "pod.netflix.com/seccomp-agent-net-enabled";
    public static final String POD_SECCOMP_AGENT_PERF_ENABLED = "pod.netflix.com/seccomp-agent-perf-enabled";
    public static final String POD_TRAFFIC_STEERING_ENABLED = "pod.netflix.com/traffic-steering-enabled";
    public static final String POD_IMAGE_TAG_PREFIX = "pod.titus.netflix.com/image-tag-";

    // Container Logging Config
    public static final String LOG_KEEP_LOCAL_FILE = "log.netflix.com/keep-local-file-after-upload";
    public static final String LOG_S3_BUCKET_NAME = "log.netflix.com/s3-bucket-name";
    public static final String LOG_S3_PATH_PREFIX = "log.netflix.com/s3-path-prefix";
    public static final String LOG_S3_WRITER_IAM_ROLE = "log.netflix.com/s3-writer-iam-role";
    public static final String LOG_STDIO_CHECK_INTERVAL = "log.netflix.com/stdio-check-interval";
    public static final String LOG_UPLOAD_THRESHOLD_TIME = "log.netflix.com/upload-threshold-time";
    public static final String LOG_UPLOAD_CHECK_INTERVAL = "log.netflix.com/upload-check-interval";
    public static final String LOG_UPLOAD_REGEXP = "log.netflix.com/upload-regexp";

    // Service Configuration
    public static final String SERVICE_PREFIX = "service.netflix.com";
}
