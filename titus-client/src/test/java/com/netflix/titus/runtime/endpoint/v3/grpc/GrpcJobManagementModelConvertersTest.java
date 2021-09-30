package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.ContainerHealth;
import com.netflix.titus.api.jobmanager.model.job.ContainerState;
import com.netflix.titus.grpc.protogen.LogLocation;
import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.grpc.protogen.TaskStatus;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_RESUBMIT_NUMBER;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTES_TASK_ORIGINAL_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_LIVE_STREAM;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_ACCOUNT_ID;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_ACCOUNT_NAME;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_BUCKET_NAME;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_KEY;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_S3_REGION;
import static com.netflix.titus.api.jobmanager.TaskAttributes.TASK_ATTRIBUTE_LOG_UI_LOCATION;
import static org.assertj.core.api.Assertions.assertThat;

public class GrpcJobManagementModelConvertersTest {

    @Test
    public void coreTaskLogAttributes() {
        String taskId = "tid-1";
        String uiLocation = "http://titus-ui/tasks/" + taskId;
        String liveStream = "http://agentIp:8080/logs/" + taskId;
        String s3Bucket = "bucket-1";
        String s3Key = "key-1";
        String s3AccountId = "acc-1";
        String s3AccountName = "acc-name-1";
        String s3Region = "us-east-1";

        Map<String, String> taskContext = new HashMap<>();
        taskContext.put(TASK_ATTRIBUTES_TASK_ORIGINAL_ID, taskId);
        taskContext.put(TASK_ATTRIBUTES_RESUBMIT_NUMBER, "1");
        Task grpcTask = Task.newBuilder()
                .setId(taskId)
                .putAllTaskContext(taskContext)
                .setLogLocation(LogLocation.newBuilder()
                        .setUi(LogLocation.UI.newBuilder().setUrl(uiLocation))
                        .setLiveStream(LogLocation.LiveStream.newBuilder().setUrl(liveStream).build())
                        .setS3(LogLocation.S3.newBuilder()
                                .setRegion(s3Region)
                                .setBucket(s3Bucket)
                                .setKey(s3Key)
                                .setAccountId(s3AccountId)
                                .setAccountName(s3AccountName))
                        .build())
                .build();

        com.netflix.titus.api.jobmanager.model.job.Task coreTask = GrpcJobManagementModelConverters.toCoreTask(grpcTask);
        assertThat(coreTask).isNotNull();
        assertThat(coreTask.getAttributes()).hasSizeGreaterThan(0);
        assertThat(coreTask.getAttributes().containsKey(TASK_ATTRIBUTE_LOG_S3_KEY)).isTrue();
        assertThat(coreTask.getAttributes().get(TASK_ATTRIBUTE_LOG_S3_KEY)).isEqualTo(s3Key);

        assertThat(coreTask.getAttributes().containsKey(TASK_ATTRIBUTE_LOG_S3_BUCKET_NAME)).isTrue();
        assertThat(coreTask.getAttributes().get(TASK_ATTRIBUTE_LOG_S3_BUCKET_NAME)).isEqualTo(s3Bucket);

        assertThat(coreTask.getAttributes().containsKey(TASK_ATTRIBUTE_LOG_S3_ACCOUNT_ID)).isTrue();
        assertThat(coreTask.getAttributes().get(TASK_ATTRIBUTE_LOG_S3_ACCOUNT_ID)).isEqualTo(s3AccountId);

        assertThat(coreTask.getAttributes().containsKey(TASK_ATTRIBUTE_LOG_S3_ACCOUNT_NAME)).isTrue();
        assertThat(coreTask.getAttributes().get(TASK_ATTRIBUTE_LOG_S3_ACCOUNT_NAME)).isEqualTo(s3AccountName);

        assertThat(coreTask.getAttributes().containsKey(TASK_ATTRIBUTE_LOG_S3_REGION)).isTrue();
        assertThat(coreTask.getAttributes().get(TASK_ATTRIBUTE_LOG_S3_REGION)).isEqualTo(s3Region);

        assertThat(coreTask.getAttributes().containsKey(TASK_ATTRIBUTE_LOG_UI_LOCATION)).isTrue();
        assertThat(coreTask.getAttributes().get(TASK_ATTRIBUTE_LOG_UI_LOCATION)).isEqualTo(uiLocation);

        assertThat(coreTask.getAttributes().containsKey(TASK_ATTRIBUTE_LOG_LIVE_STREAM)).isTrue();
        assertThat(coreTask.getAttributes().get(TASK_ATTRIBUTE_LOG_LIVE_STREAM)).isEqualTo(liveStream);
    }


    @Test
    public void testToGrpc() {
        TaskStatus.ContainerState containerState = GrpcJobManagementModelConverters.toGrpcContainerState(ContainerState.newBuilder()
                .withContainerName("test").withContainerHealth(ContainerHealth.Unset).build());
        assertThat(containerState.isInitialized()).isTrue();
    }
}