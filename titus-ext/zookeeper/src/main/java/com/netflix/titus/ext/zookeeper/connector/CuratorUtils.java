package com.netflix.titus.ext.zookeeper.connector;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import rx.Completable;

import static org.apache.zookeeper.KeeperException.Code.OK;

public class CuratorUtils {

    public static boolean createPathIfNotExist(CuratorFramework curator, String fullPath, boolean persisted) {
        try {
            Stat pathStat = curator.checkExists().forPath(fullPath);
            // Create the path only if the path does not exist
            if (pathStat != null) {
                return false;
            }
            curator.create()
                    .creatingParentsIfNeeded()
                    .withMode(persisted ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL)
                    .forPath(fullPath);
            return true;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot create Zookeeper path: " + fullPath, e);
        }
    }

    public static Completable setData(CuratorFramework curator, String path, byte[] data) {
        return Completable.fromEmitter(emitter -> {
            try {
                curator
                        .setData()
                        .inBackground((client, event) -> {
                            if (event.getResultCode() == OK.intValue()) {
                                emitter.onCompleted();
                            } else {
                                emitter.onError(new IOException(String.format("Failed to store data in zookeeper node: path=%s, event=%s", path, event)));
                            }
                        }).forPath(path, data);
            } catch (Exception e) {
                emitter.onError(new IOException(String.format("Unexpected error when storing data in zookeeper node: path=%s, error=%s", path, e.getMessage()), e));
            }
        });
    }
}
