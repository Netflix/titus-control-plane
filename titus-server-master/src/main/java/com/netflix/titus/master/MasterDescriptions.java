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

package com.netflix.titus.master;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.api.supervisor.service.MasterDescription;

/**
 */
public final class MasterDescriptions {

    private MasterDescriptions() {
    }

    public static MasterDescription create(MasterConfiguration masterConfiguration) {
        return create(
                masterConfiguration.getMasterHost(),
                masterConfiguration.getMasterIP(),
                masterConfiguration.getApiProxyPort(),
                masterConfiguration.getApiStatusUri()
        );
    }

    public static MasterDescription create(String masterHost, String masterIp, int apiPort, String apiStatusUri) {
        return new MasterDescription(
                getHost(masterHost),
                getHostIP(masterIp),
                apiPort,
                apiStatusUri,
                System.currentTimeMillis()
        );
    }

    private static String getHost(String host) {
        if (host != null && !host.isEmpty()) {
            return host;
        }

        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }

    private static String getHostIP(String masterIp) {
        String ip = masterIp;
        if (ip != null && !ip.isEmpty()) {
            return ip;
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to get the host information: " + e.getMessage(), e);
        }
    }
}
