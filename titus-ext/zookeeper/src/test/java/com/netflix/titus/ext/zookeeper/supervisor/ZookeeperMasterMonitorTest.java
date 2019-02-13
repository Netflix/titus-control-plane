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

package com.netflix.titus.ext.zookeeper.supervisor;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.ext.zookeeper.CuratorServiceResource;
import com.netflix.titus.ext.zookeeper.ZookeeperPaths;
import com.netflix.titus.ext.zookeeper.ZookeeperTestUtils;
import com.netflix.titus.ext.zookeeper.connector.CuratorUtils;
import com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterInstanceFunctions;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.MasterStatus;
import com.netflix.titus.master.supervisor.service.MasterDescription;
import com.netflix.titus.testkit.junit.category.IntegrationNotParallelizableTest;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.ext.zookeeper.ZookeeperTestUtils.newMasterDescription;
import static com.netflix.titus.master.supervisor.endpoint.grpc.SupervisorGrpcModelConverters.toCoreMasterInstance;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Category(IntegrationNotParallelizableTest.class)
public class ZookeeperMasterMonitorTest {

    private static final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private static final MasterInstance DEFAULT_MASTER_INSTANCE = toCoreMasterInstance(com.netflix.titus.grpc.protogen.MasterInstance.getDefaultInstance());

    @ClassRule
    public static CuratorServiceResource curatorServiceResource = new CuratorServiceResource(titusRuntime);

    private static CuratorFramework curator;

    private static ZookeeperPaths zkPaths;

    private ZookeeperMasterMonitor masterMonitor;

    @BeforeClass
    public static void setUpClass() {
        curatorServiceResource.createAllPaths();
        curator = curatorServiceResource.getCuratorService().getCurator();
        zkPaths = curatorServiceResource.getZkPaths();
    }

    @Before
    public void setUp() {
        masterMonitor = new ZookeeperMasterMonitor(zkPaths, curatorServiceResource.getCuratorService(), titusRuntime);
        masterMonitor.start();
    }

    @After
    public void tearDown() {
        masterMonitor.shutdown();
    }

    @Test(timeout = 30_000)
    public void testMonitorWorksForMultipleLeaderUpdates() throws Exception {
        // Note we intentionally didn't set the initial value of master description because we'd like to make sure
        // that the monitor will work property even if it fails occasionally (in this case, it will fail to deserialize
        // the master description in the very beginning
        ExtTestSubscriber<MasterDescription> leaderSubscriber = new ExtTestSubscriber<>();
        masterMonitor.getLeaderObservable().filter(Objects::nonNull).subscribe(leaderSubscriber);

        for (int i = 0; i < 5; i++) {
            curator.setData()
                    .forPath(zkPaths.getLeaderAnnouncementPath(), ObjectMappers.defaultMapper().writeValueAsBytes(newMasterDescription(i)));

            // Try a few times, as we can get update for the same entity more than once.
            for (int j = 0; j < 3; j++) {
                MasterDescription newLeader = leaderSubscriber.takeNext(5, TimeUnit.SECONDS);
                if (newLeader != null && newLeader.getApiPort() == i) {
                    return;
                }
            }
            fail("Did not received TitusMaster update for iteration " + i);
        }
    }

    @Test(timeout = 30_000)
    public void testLocalMasterInstanceUpdates() throws Exception {
        // Should get dummy version first.
        assertThat(masterMonitor.getCurrentMasterInstance()).isEqualTo(ZookeeperMasterMonitor.UNKNOWN_MASTER_INSTANCE);

        ExtTestSubscriber<List<MasterInstance>> mastersSubscriber = new ExtTestSubscriber<>();
        masterMonitor.observeMasters().subscribe(mastersSubscriber);
        assertThat(mastersSubscriber.takeNext()).isEmpty();

        // Update information about itself
        MasterInstance initial = ZookeeperTestUtils.newMasterInstance("selfId", MasterState.Inactive);
        assertThat(masterMonitor.updateOwnMasterInstance(initial).get()).isNull();
        expectMasters(mastersSubscriber, initial);

        // Change state
        MasterInstance updated = MasterInstanceFunctions.moveTo(initial, MasterStatus.newBuilder()
                .withState(MasterState.NonLeader)
                .withReasonCode("nextStep")
                .withReasonMessage("testing")
                .build()
        );
        assertThat(masterMonitor.updateOwnMasterInstance(updated).get()).isNull();
        expectMasters(mastersSubscriber, updated);

        // Now add second master
        MasterInstance second = ZookeeperTestUtils.newMasterInstance("secondId", MasterState.Inactive);

        addMasterInstanceToZookeeper(second);
        expectMasters(mastersSubscriber, updated, second);

        // And remove it
        removeMasterInstanceFromZookeeper(second.getInstanceId());
        expectMasters(mastersSubscriber, updated);
    }

    /**
     * Zookeeper sends multiple events for the same master update, so we have to be able to filter this out.
     */
    private void expectMasters(ExtTestSubscriber<List<MasterInstance>> mastersSubscriber, MasterInstance... masters) throws Exception {
        List<MasterInstance> last = null;
        for (List<MasterInstance> next; (next = mastersSubscriber.takeNext()) != null; ) {
            // Due to race condition we may get here default protobuf MasterInstance value (empty ZK node), followed
            // by the initial value set after the ZK path is created.
            long emptyNodeCount = next.stream().filter(m -> m.equals(DEFAULT_MASTER_INSTANCE)).count();
            if (emptyNodeCount == 0) {
                last = next;
            }
        }
        boolean matches = last != null && new HashSet<>(last).equals(asSet(masters));
        if (!matches) {
            assertThat(mastersSubscriber.takeNext(5, TimeUnit.SECONDS)).hasSize(masters.length).contains(masters);
        }
    }

    private String buildInstancePath(String instanceId) {
        return zkPaths.getAllMastersPath() + "/" + instanceId;
    }

    private void addMasterInstanceToZookeeper(MasterInstance instance) {
        String path = buildInstancePath(instance.getInstanceId());
        CuratorUtils.createPathIfNotExist(curator, path, false);

        byte[] bytes = SupervisorGrpcModelConverters.toGrpcMasterInstance(instance).toByteArray();
        assertThat(CuratorUtils.setData(curator, path, bytes).get()).isNull();
    }

    private void removeMasterInstanceFromZookeeper(String instanceId) throws Exception {
        curator.delete().inBackground((client, event) -> {
            System.out.printf("Received Curator notification after the instance node %s was removed: %s", instanceId, event);
        }).forPath(buildInstancePath(instanceId));
    }
}