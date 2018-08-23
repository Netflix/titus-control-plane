package com.netflix.titus.ext.zookeeper;

import java.io.File;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.ext.zookeeper.connector.CuratorService;
import com.netflix.titus.ext.zookeeper.connector.CuratorServiceImpl;
import com.netflix.titus.ext.zookeeper.connector.CuratorUtils;
import com.netflix.titus.ext.zookeeper.connector.DefaultZookeeperClusterResolver;
import com.netflix.titus.ext.zookeeper.connector.ZookeeperClusterResolver;
import org.I0Itec.zkclient.ZkServer;
import org.apache.curator.framework.CuratorFramework;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class CuratorServiceResource extends ExternalResource {

    private static final String ZK_SERVER_HOST = "127.0.0.1";

    private final TemporaryFolder tempFolder;
    private final TitusRuntime titusRuntime;

    private ZookeeperPaths zkPaths;

    private ZookeeperServer zkServer;
    private CuratorServiceImpl curatorService;

    public CuratorServiceResource(TitusRuntime titusRuntime) {
        this.tempFolder = new TemporaryFolder();
        this.titusRuntime = titusRuntime;
    }

    @Override
    protected void before() throws Throwable {
        File rootFolder = tempFolder.newFolder();
        String rootPath = rootFolder.getAbsolutePath();
        String dataPath = rootPath + "/data";
        String logPath = rootPath + "/log";
        zkServer = new ZookeeperServer(
                "localhost",
                dataPath,
                logPath,
                zkClient -> {
                },
                0,
                ZkServer.DEFAULT_TICK_TIME, 100);
        String zkConnectStr = String.format("%s:%d", ZK_SERVER_HOST, zkServer.getPort());
        zkServer.start();


        ZookeeperConfiguration zookeeperConfiguration = ZookeeperTestUtils.withEmbeddedZookeeper(Mockito.mock(ZookeeperConfiguration.class), zkConnectStr);
        zkPaths = new ZookeeperPaths(zookeeperConfiguration);

        ZookeeperClusterResolver clusterResolver = new DefaultZookeeperClusterResolver(zookeeperConfiguration);
        curatorService = new CuratorServiceImpl(zookeeperConfiguration, clusterResolver, titusRuntime.getRegistry());

        curatorService.start();
    }

    @Override
    protected void after() {
        curatorService.shutdown();
        zkServer.shutdown();
    }

    public void createAllPaths() {
        CuratorFramework curator = curatorService.getCurator();
        CuratorUtils.createPathIfNotExist(curator, zkPaths.getLeaderElectionPath(), true);
        CuratorUtils.createPathIfNotExist(curator, zkPaths.getLeaderAnnouncementPath(), true);
    }

    public CuratorService getCuratorService() {
        return curatorService;
    }

    public ZookeeperPaths getZkPaths() {
        return zkPaths;
    }
}
