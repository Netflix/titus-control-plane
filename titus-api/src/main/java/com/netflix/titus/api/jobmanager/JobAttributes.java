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

package com.netflix.titus.api.jobmanager;

/**
 * Constant keys for Job attributes. Attributes that begin with <b>titus.</b> are readonly system generated attributes
 * while attributes that begin with <b>titusParameter.</b> are user supplied parameters.
 */
public final class JobAttributes {

    public static final String TITUS_ATTRIBUTE_PREFIX = "titus.";
    public static final String TITUS_PARAMETER_ATTRIBUTE_PREFIX = "titusParameter.";
    public static final String TITUS_PARAMETER_AGENT_PREFIX = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.";
    public static final String JOB_ATTRIBUTE_SANITIZATION_PREFIX = TITUS_ATTRIBUTE_PREFIX + "sanitization.";
    public static final String PREDICTION_ATTRIBUTE_PREFIX = TITUS_ATTRIBUTE_PREFIX + "runtimePrediction.";

    // Job Descriptor Attributes

    /**
     * Job creator.
     */
    public static final String JOB_ATTRIBUTES_CREATED_BY = TITUS_ATTRIBUTE_PREFIX + "createdBy";

    /**
     * Federated stack name. All cells under the same federated stack must share the same value.
     */
    public static final String JOB_ATTRIBUTES_STACK = TITUS_ATTRIBUTE_PREFIX + "stack";

    /**
     * Unique Cell name for a deployment that the Job has been assigned to.
     */
    public static final String JOB_ATTRIBUTES_CELL = TITUS_ATTRIBUTE_PREFIX + "cell";

    /**
     * This attribute specifies the destination cell the federation is configured to route the API request to.
     */
    public static final String JOB_ATTRIBUTE_ROUTING_CELL = TITUS_ATTRIBUTE_PREFIX + "routingCell";

    /**
     * Job id. When this attribute is present it signals the control plane that the id was created by federation
     * and should be used instead of minting a new value for any CreateJob API calls.
     */
    public static final String JOB_ATTRIBUTES_FEDERATED_JOB_ID = TITUS_ATTRIBUTE_PREFIX + "federatedJobId";

    /**
     * This attribute is used to persist original federated Job id that may have been used to populate job id.
     */
    public static final String JOB_ATTRIBUTES_ORIGINAL_FEDERATED_JOB_ID = TITUS_ATTRIBUTE_PREFIX + "originalFederatedJobId";

    /**
     * Set to true when sanitization for iam roles fails open
     */
    public static final String JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IAM = JOB_ATTRIBUTE_SANITIZATION_PREFIX + "skipped.iam";

    /**
     * Set to true when sanitization for container images (digest) fails open
     */
    public static final String JOB_ATTRIBUTES_SANITIZATION_SKIPPED_IMAGE = JOB_ATTRIBUTE_SANITIZATION_PREFIX + "skipped.image";

    /**
     * Set to true when sanitization for container EBS volumes fails open
     */
    public static final String JOB_ATTRIBUTES_SANITIZATION_SKIPPED_EBS = JOB_ATTRIBUTE_SANITIZATION_PREFIX + "skipped.ebs";

    /**
     * Set to true when job runtime prediction fails open
     */
    public static final String JOB_ATTRIBUTES_SANITIZATION_SKIPPED_RUNTIME_PREDICTION = JOB_ATTRIBUTE_SANITIZATION_PREFIX + "skipped.runtimePrediction";

    /**
     * Information about the prediction selector used in the evaluation process.
     */
    public static final String JOB_ATTRIBUTES_RUNTIME_PREDICTION_SELECTOR_INFO = PREDICTION_ATTRIBUTE_PREFIX + "selectorInfo";

    /**
     * Predicted runtime for a particular job in seconds, used in opportunistic CPU scheduling
     */
    public static final String JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC = PREDICTION_ATTRIBUTE_PREFIX + "predictedRuntimeSec";

    /**
     * Confidence of the predicted runtime for a particular job
     */
    public static final String JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE = PREDICTION_ATTRIBUTE_PREFIX + "confidence";

    /**
     * Metadata (identifier) about what model generated predictions for a particular job
     */
    public static final String JOB_ATTRIBUTES_RUNTIME_PREDICTION_MODEL_ID = PREDICTION_ATTRIBUTE_PREFIX + "modelId";

    /**
     * Metadata (version) of the service that generated predictions for a particular job
     */
    public static final String JOB_ATTRIBUTES_RUNTIME_PREDICTION_VERSION = PREDICTION_ATTRIBUTE_PREFIX + "version";

    /**
     * All available runtime predictions with their associated confidence, as returned from the predictions service for
     * a particular job.
     * <p>
     * These are informational, if any particular prediction is selected for a job, it will be reflected by
     * {@link JobAttributes#JOB_ATTRIBUTES_RUNTIME_PREDICTION_SEC} and {@link JobAttributes#JOB_ATTRIBUTES_RUNTIME_PREDICTION_CONFIDENCE}.
     */
    public static final String JOB_ATTRIBUTES_RUNTIME_PREDICTION_AVAILABLE = PREDICTION_ATTRIBUTE_PREFIX + "available";

    /**
     * AB test metadata associated with a prediction.
     */
    public static final String JOB_ATTRIBUTES_RUNTIME_PREDICTION_AB_TEST = PREDICTION_ATTRIBUTE_PREFIX + "abTest";

    /**
     * Do not parse entry point into shell command, argument list
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_ENTRY_POINT_SKIP_SHELL_PARSING = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "entryPoint.skipShellParsing";

    /**
     * Allow jobs to completely opt-out of having their runtime automatically predicted during admission
     */
    public static final String JOB_PARAMETER_SKIP_RUNTIME_PREDICTION = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "runtimePrediction.skip";

    /**
     * (Experimental) allow jobs to request being placed into a particular cell (affinity)
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_CELL_REQUEST = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "cell.request";

    /**
     * (Experimental) allow jobs to request not being placed into a comma separated list of cell names (anti affinity)
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_CELL_AVOID = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "cell.avoid";

    /**
     * A comma separated list specifying which taints this job can tolerate during scheduling. This job must be able to tolerate
     * all taints in order to be scheduled on that machine.
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_TOLERATIONS = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "tolerations";

    public static final String JOB_PARAMETER_RESOURCE_POOLS = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "resourcePools";

    /**
     * Informs a job intent to allow containers that are running on bad agents to be terminated
     */
    public static final String JOB_PARAMETER_TERMINATE_ON_BAD_AGENT = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "terminateContainerOnBadAgent";

    /*
     * Job security attributes.
     */

    /**
     * Prefix for attributes derived from Job Metatron details.
     */
    public static final String METATRON_ATTRIBUTE_PREFIX = "metatron.";

    /**
     * The Metatron auth context received from Metatron for the metatron signed job.
     */
    public static final String JOB_SECURITY_ATTRIBUTE_METATRON_AUTH_CONTEXT = METATRON_ATTRIBUTE_PREFIX + "authContext";

    // Container Attributes

    /**
     * Allow the task to use more CPU (as based on time slicing) than specified.
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_ALLOW_CPU_BURSTING =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.allowCpuBursting";

    /**
     * Allow the task to use more Network (as based on time and bandwidth slicing) than specified.
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_BURSTING =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.allowNetworkBursting";

    /**
     * Elastic IP pool
     */
    public static final String JOB_PARAMETER_ATTRIBUTE_EIP_POOL =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.elasticIPPool";

    /**
     * Elastic IPs
     */
    public static final String JOB_PARAMETER_ATTRIBUTE_EIPS =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.elasticIPs";

    /**
     * Sets SCHED_BATCH -- Linux batch scheduling, for cache-friendly handling of lowprio, batch-like, CPU-bound, 100% non-interactive tasks.
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_SCHED_BATCH =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.schedBatch";

    /**
     * Allows the creation of nested containers.
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_ALLOW_NESTED_CONTAINERS =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.allowNestedContainers";

    /**
     * How long to wait for a task (container) to exit on its own after sending a SIGTERM -- after this period elapses, a SIGKILL will sent.
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_KILL_WAIT_SECONDS =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.killWaitSeconds";

    /**
     * Require a token or not on the imds
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_IMDS_REQUIRE_TOKEN =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.imds.requireToken";

    /*
     * Log location container attributes (set in {@link Container#getAttributes()}.
     */

    /**
     * Custom S3 bucket log location.
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_S3_BUCKET_NAME = "titusParameter.agent.log.s3BucketName";

    /**
     * Custom S3 bucket path prefix.
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_S3_PATH_PREFIX = "titusParameter.agent.log.s3PathPrefix";

    /**
     * If a user specifies a custom bucket location, the S3 writer role will include the container's IAM role.
     * Otherwise default role will be used which has access to a default S3 bucket.
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_S3_WRITER_ROLE = "titusParameter.agent.log.s3WriterRole";

    /**
     * Subnets to launch the container in.
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_SUBNETS = "titusParameter.agent.subnets";

    /**
     * AccountId to launch the container in.
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_ACCOUNT_ID = "titusParameter.agent.accountId";

    /**
     * Enable service mesh.
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_ENABLED = "titusParameter.agent.service.serviceMesh.enabled";

    /**
     * Container to use for service mesh.
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_SERVICEMESH_CONTAINER = "titusParameter.agent.service.serviceMesh.container";

    /**
     * Set to true when sanitization for container images (digest) fails open
     */
    public static final String JOB_ATTRIBUTES_SANITIZATION_SKIPPED_SERVICEMESH_IMAGE = JOB_ATTRIBUTE_SANITIZATION_PREFIX + "skipped.serviceMesh";

    /**
     * Enable TSA for perf-related syscalls
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_SECCOMP_AGENT_PERF_ENABLED =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.seccompAgentEnabledForPerfSyscalls";
    /**
     * Enable TSA for network-related syscalls
     */
    public static final String JOB_CONTAINER_ATTRIBUTE_SECCOMP_AGENT_NET_ENABLED =
            TITUS_PARAMETER_ATTRIBUTE_PREFIX + "agent.seccompAgentEnabledForNetSyscalls";

    /*
     * EBS volume job attributes (set in {@link JobDescriptor#getAttributes()}.
     * EBS volume IDs are attributes as we do not expect to expose them directly as first-class Titus API
     * constructs but rather wait until the Kubernetes API to expose them directly.
     * We expect the value to be a comma separated list of valid/existing EBS volume IDs.
     */

    public static final String JOB_ATTRIBUTES_EBS_VOLUME_IDS = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "ebs.volumeIds";

    public static final String JOB_ATTRIBUTES_EBS_MOUNT_POINT = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "ebs.mountPoint";

    public static final String JOB_ATTRIBUTES_EBS_MOUNT_PERM = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "ebs.mountPerm";

    public static final String JOB_ATTRIBUTES_EBS_FS_TYPE = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "ebs.fsType";

    /*
     * Job topology spreading.
     */

    /**
     * By default job spreading is disabled for batch jobs and enabled for service jobs. The default behavior can
     * be overridden with this property.
     */
    public static final String JOB_ATTRIBUTES_SPREADING_ENABLED = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "spreading.enabled";

    /**
     * Overrides the default max skew for a job topology spreading. For a job with availability percentage limit disruption
     * budget policy, the skew is computed as a percentage of pods that can be taken down. For other disruption budgets
     * it is set to 1. Job topology spreading is a soft constraint, so violating tke max skew constraint does not
     * prevent a pod from being scheduled.
     */
    public static final String JOB_ATTRIBUTES_SPREADING_MAX_SKEW = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "spreading.maxSkew";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_HOSTNAME_STYLE = TITUS_PARAMETER_AGENT_PREFIX + "hostnameStyle";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_ALLOW_NETWORK_JUMBO = TITUS_PARAMETER_AGENT_PREFIX + "allowNetworkJumbo";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_FUSE_ENABLED = TITUS_PARAMETER_AGENT_PREFIX + "fuseEnabled";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_ASSIGN_IPV6_ADDRESS = TITUS_PARAMETER_AGENT_PREFIX + "assignIPv6Address";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_CHECK_INTERVAL = TITUS_PARAMETER_AGENT_PREFIX + "log.uploadCheckInterval";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_LOG_STDIO_CHECK_INTERVAL = TITUS_PARAMETER_AGENT_PREFIX + "log.stdioCheckInterval";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_THRESHOLD_TIME = TITUS_PARAMETER_AGENT_PREFIX + "log.uploadThresholdTime";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_LOG_KEEP_LOCAL_FILE_AFTER_UPLOAD = TITUS_PARAMETER_AGENT_PREFIX + "log.keepLocalFileAfterUpload";

    /**
     *
     */
    public static final String JOB_PARAMETER_ATTRIBUTES_LOG_UPLOAD_REGEXP = TITUS_PARAMETER_AGENT_PREFIX + "log.uploadRegexp";

    private JobAttributes() {
    }
}
