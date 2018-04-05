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

package com.netflix.titus.master.endpoint.v2.rest;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.netflix.titus.master.master.MasterDescription;

public class Util {

    public static boolean isLocalHost(MasterDescription master) {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            for (InetAddress addr : InetAddress.getAllByName(master.getHostname())) {
                if (addr.equals(localHost)) {
                    return true;
                }
            }
        } catch (UnknownHostException e) {
            //logger.warn("Failed to compare if given master {} is local host: {}", master, e);
            return false;
        }

        return false;
    }
}
