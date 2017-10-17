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

package io.netflix.titus.master.cluster;

import java.io.File;

import org.I0Itec.zkclient.ZkServer;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class ZkExternalResource extends ExternalResource {

    private static final String ZK_SERVER_HOST = "127.0.0.1";

    private final TemporaryFolder tempFolder;

    private ZookeeperServer zkServer;
    private String zkConnectStr;

    public ZkExternalResource(TemporaryFolder tempFolder) {
        this.tempFolder = tempFolder;
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
        zkConnectStr = String.format("%s:%d", ZK_SERVER_HOST, zkServer.getPort());
        zkServer.start();
    }

    @Override
    protected void after() {
        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    public String getZkConnStr() {
        return zkConnectStr;
    }
}