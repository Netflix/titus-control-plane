package com.netflix.titus.master.integration.v3.supervisor;

import java.util.Iterator;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.MasterInstance;
import com.netflix.titus.grpc.protogen.MasterInstanceId;
import com.netflix.titus.grpc.protogen.MasterInstances;
import com.netflix.titus.grpc.protogen.MasterStatus;
import com.netflix.titus.grpc.protogen.SupervisorEvent;
import com.netflix.titus.grpc.protogen.SupervisorServiceGrpc.SupervisorServiceBlockingStub;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class SupervisorBasicTest extends BaseIntegrationTest {

    @ClassRule
    public static final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusCells.basicCell(1));

    private final SupervisorServiceBlockingStub blockingGrpcClient = titusStackResource.getMaster().getSupervisorBlockingGrpcClient();

    @Test
    public void testGetMasterInstances() {
        MasterInstances instances = blockingGrpcClient.getMasterInstances(Empty.getDefaultInstance());
        assertThat(instances.getInstancesList()).hasSize(1);

        MasterInstance first = instances.getInstances(0);
        MasterInstance instance = blockingGrpcClient.getMasterInstance(MasterInstanceId.newBuilder().setInstanceId(first.getInstanceId()).build());
        assertThat(instance.getStatus().getState()).isEqualTo(MasterStatus.MasterState.LeaderActivated);
    }

    @Test
    public void testObserveEvents() {
        Iterator<SupervisorEvent> it = blockingGrpcClient.observeEvents(Empty.getDefaultInstance());

        SupervisorEvent next = it.next();
        assertThat(next.getEventCase()).isEqualTo(SupervisorEvent.EventCase.MASTERINSTANCEUPDATE);
    }
}
