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

package io.netflix.titus.testkit.perf.load;

import java.io.PrintWriter;
import java.util.Map;
import java.util.function.UnaryOperator;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.config.MapConfig;
import com.netflix.archaius.guice.ArchaiusModule;
import com.netflix.governator.guice.jersey.GovernatorJerseySupportModule;
import com.netflix.governator.guice.jersey.GovernatorServletContainer;
import com.netflix.governator.guice.jetty.Archaius2JettyModule;
import com.netflix.governator.providers.Advises;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import io.netflix.titus.runtime.endpoint.common.rest.JsonMessageReaderWriter;
import io.netflix.titus.testkit.client.DefaultTitusMasterClient;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.perf.load.rest.ReportResource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.PropertyConfigurator;

/**
 */
public class LoadGenerator {

    static {
        PropertyConfigurator.configure(LoadGenerator.class.getClassLoader().getResource("load-log4j.properties"));
    }

    private final Injector injector;

    public LoadGenerator(String[] args) throws Exception {
        CommandLine cli = parseOptions(args);

        String hostName = cli.getOptionValue('H');
        int port = getIntOpt(cli, 'p', 7001);
        boolean clean = getOptions().hasOption("c");
        int scaleFactor = getIntOpt(cli, 's', 1);
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .put("scaleFactor", Integer.toString(scaleFactor))
                .put("clean", Boolean.toString(clean))
                .build();

        this.injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                install(new ArchaiusModule() {
                    @Override
                    protected void configureArchaius() {
                        bindDefaultConfig().toInstance(MapConfig.builder()
                                .put("governator.jetty.embedded.port", "8999")
                                .put("governator.jetty.embedded.webAppResourceBase", "/")
                                .build());

                        bindConfigReader().toInstance(MapConfig.from(config));
                    }
                });

                bind(TitusMasterClient.class).toInstance(new DefaultTitusMasterClient(hostName, port));

                install(new Archaius2JettyModule());
                install(new GovernatorJerseySupportModule());
                install(new JerseyServletModule() {
                    @Override
                    protected void configureServlets() {
                        // This sets up Jersey to serve any found resources that start with the base path of "/*"
                        serve("/*").with(GovernatorServletContainer.class);
                    }

                    @Advises
                    @Singleton
                    @Named("governator")
                    UnaryOperator<DefaultResourceConfig> getConfig() {
                        return config -> {
                            // Providers
                            config.getClasses().add(JsonMessageReaderWriter.class);

                            // Resources
                            config.getClasses().add(ReportResource.class);
                            return config;
                        };
                    }

                });
            }

            @Provides
            @Singleton
            public LoadConfiguration getLoadConfiguration(ConfigProxyFactory factory) {
                return factory.newProxy(LoadConfiguration.class);
            }
        });
    }

    private void tearDown() {
    }

    void run() throws InterruptedException {
        injector.getInstance(Orchestrator.class).awaitTermination();
    }

    private static int getIntOpt(CommandLine cli, char opt, long defaultValue) throws ParseException {
        return (int) (cli.hasOption(opt) ? (long) cli.getParsedOptionValue(Character.toString(opt)) : defaultValue);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(Option.builder("H").longOpt("host").argName("host_name").hasArg().required()
                .desc("TitusMaster host name")
                .build());
        options.addOption(Option.builder("p").longOpt("port").argName("port_number").hasArg().type(Number.class)
                .desc("TitusMaster port number (default 7001)")
                .build());
        options.addOption(Option.builder("c").longOpt("clean")
                .desc("Remove jobs from previous sessions")
                .build());
        options.addOption(Option.builder("s").longOpt("scale").argName("number").hasArg().type(Number.class)
                .desc("Scale factor")
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

        writer.println("Usage: LoadGenerator [params]");
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
        LoadGenerator perf = null;
        try {
            perf = new LoadGenerator(args);
        } catch (Throwable e) {
            System.err.println("ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            perf.run();
        } catch (Exception e) {
            System.err.println("ERROR: test run failure");
            e.printStackTrace();
        } finally {
            perf.tearDown();
        }
    }
}
