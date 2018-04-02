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

package com.netflix.titus.testkit.perf.embedded;

import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import com.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import com.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import com.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import rx.Observable;

/**
 * Performance tests runner for embedded TitusMaster.
 */
public class TitusMasterPerf {

    private final int clusters;
    private final int clusterSize;
    private final int testSuiteId;
    private final int loadLevel;

    private EmbeddedTitusMaster titusMaster;
    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator("perfTests");

    public TitusMasterPerf(String[] args) throws Exception {
        CommandLine cli = parseOptions(args);
        this.clusters = getIntOpt(cli, 'c', 1);
        this.clusterSize = getIntOpt(cli, 's', 5);
        this.testSuiteId = getIntOpt(cli, 't', 0);
        this.loadLevel = getIntOpt(cli, 'l', 1);
    }

    private static int getIntOpt(CommandLine cli, char opt, long defaultValue) throws ParseException {
        return (int) (cli.hasOption(opt) ? (long) cli.getParsedOptionValue(Character.toString(opt)) : defaultValue);
    }

    private void setUp() throws Throwable {
        EmbeddedTitusMaster.Builder builder = EmbeddedTitusMaster.aTitusMaster();
        for (int i = 0; i < clusters; i++) {
            builder.withAgentCluster(
                    SimulatedTitusAgentCluster.aTitusAgentCluster("testAgentCluser_" + i, i)
                            .withSize(clusterSize)
                            .withInstanceType(AwsInstanceType.M4_10XLarge)
            );
        }
        titusMaster = builder.build();
        titusMaster.boot();
    }

    private void tearDown() {
        titusMaster.shutdown();
    }

    private void execute() throws InterruptedException {
        switch (testSuiteId) {
            case 0:
                executeTestSuite0();
                break;
            default:
                System.out.println("ERROR: invalid test suite id");
                return;
        }
        Thread.sleep(60 * 60 * 1000);
    }

    /**
     * Run short running (5sec) batch jobs.
     */
    private void executeTestSuite0() {
        int jobDurationSec = 5;
        double jobRateSec = clusters * clusterSize / jobDurationSec * loadLevel;
        long jobIntervalMs = (long) (1000 / jobRateSec);

        AtomicInteger jobIdx = new AtomicInteger();
        Observable.interval(0, jobIntervalMs, TimeUnit.MILLISECONDS).subscribe(
                next -> scheduleBatchJob(jobIdx.getAndIncrement(), jobDurationSec)
        );
    }

    private void scheduleBatchJob(int idx, int jobDurationSec) {
        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.batch, "myjob#" + idx))
                .instances(1)
                .cpu(1)
                .build();
        new BatchJobActor(jobSpec, jobDurationSec, titusMaster)
                .execute()
                .subscribe(
                        jobId -> {
                            System.out.println("Finished job " + jobId);
                        },
                        e -> {
                            System.out.println("ERROR: batch job execution error");
                            e.printStackTrace();
                        }
                );
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("c").longOpt("clusters").argName("number").hasArg().type(Number.class)
                .desc("Number of Titus agent clusters to run")
                .build());
        options.addOption(Option.builder("s").longOpt("cluster-size").argName("number").hasArg().type(Number.class)
                .desc("Number of concurrent clients")
                .build());
        options.addOption(Option.builder("t").longOpt("test-suite").argName("number").hasArg().type(Number.class)
                .desc("Id of test suite to run")
                .build());
        options.addOption(Option.builder("l").longOpt("load").argName("number").hasArg().type(Number.class)
                .desc("Load level (1...)")
                .build());
        return options;
    }

    private static CommandLine parseOptions(String[] args) {
        Options options = getOptions();

        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static boolean hasHelpOption(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-h") || args[i].equals("--help")) {
                return true;
            }
        }
        return false;
    }

    private static void printHelp() {
        PrintWriter writer = new PrintWriter(System.out);
        HelpFormatter formatter = new HelpFormatter();

        writer.println("Usage: TitusMasterPerf [params]");
        writer.println();
        writer.println("Options");
        formatter.printOptions(writer, 128, getOptions(), 4, 4);
        writer.println();
        writer.flush();
    }

    public static void main(String[] args) {
        if (hasHelpOption(args)) {
            printHelp();
            return;
        }
        TitusMasterPerf perf = null;
        try {
            perf = new TitusMasterPerf(args);
        } catch (Throwable e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            perf.setUp();
        } catch (Throwable e) {
            System.err.println("ERROR: cluster bootstrapping failure");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            perf.execute();
        } catch (Exception e) {
            System.err.println("ERROR: test run failure");
            e.printStackTrace();
        } finally {
            perf.tearDown();
        }
    }
}
