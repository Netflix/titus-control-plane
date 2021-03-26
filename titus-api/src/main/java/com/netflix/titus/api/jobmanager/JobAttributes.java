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
     * Federated job id. The federated job id is inherited by its child jobs in each cell.
     */
    public static final String JOB_ATTRIBUTES_JOB_ID= TITUS_ATTRIBUTE_PREFIX + "jobId";

    public static final String JOB_ATTRIBUTES_ROUTING_CELL = TITUS_ATTRIBUTE_PREFIX + "routingCell";

    /**
     * Unique Cell name for a deployment that the Job has been assigned to.
     */
    public static final String JOB_ATTRIBUTES_CELL = TITUS_ATTRIBUTE_PREFIX + "cell";

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
     * Request that a job is not migrated to KubeScheduler.
     */
    public static final String JOB_PARAMETER_NO_KUBE_SCHEDULER_MIGRATION = TITUS_PARAMETER_ATTRIBUTE_PREFIX + "noKubeSchedulerMigration";

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

    private JobAttributes() {
    }
}
