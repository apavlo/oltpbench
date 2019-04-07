/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.oltpbenchmark.api.*;
import com.oltpbenchmark.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.log4j.Logger;

import com.oltpbenchmark.types.DatabaseType;

public class OLTPBench {
    private static final Logger LOG = Logger.getLogger(OLTPBench.class);
    
    private static final String SINGLE_LINE = StringUtil.repeat("=", 70);
    
    public static final String RATE_DISABLED = "disabled";
    public static final String RATE_UNLIMITED = "unlimited";
    
    /**
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        // Initialize log4j
        String log4jPath = System.getProperty("log4j.configuration");
        if (log4jPath != null) {
            org.apache.log4j.PropertyConfigurator.configure(log4jPath);
        } else {
            throw new RuntimeException("Missing log4j.properties file");
        }
        
        if (ClassUtil.isAssertsEnabled()) {
            LOG.warn("\n" + getAssertWarning());
        }

        OLTPBenchOptions options = new OLTPBenchOptions();

        // create the command line parser
        CommandLineParser parser = new PosixParser();
        try {
            options.pluginConfig = new XMLConfiguration("config/plugin.xml");
        } catch (ConfigurationException e1) {
            LOG.info("Plugin configuration file config/plugin.xml is missing");
            e1.printStackTrace();
        }
        options.pluginConfig.setExpressionEngine(new XPathExpressionEngine());
        Options cmdlineOptions = new Options();
        cmdlineOptions.addOption(
                "b",
                "bench",
                true,
                "[required] Benchmark class. Currently supported: "+ options.pluginConfig.getList("/plugin//@name"));
        cmdlineOptions.addOption(
                "c", 
                "config", 
                true,
                "[required] Workload configuration file");
        cmdlineOptions.addOption(
                null,
                "create",
                true,
                "Initialize the database for this benchmark");
        cmdlineOptions.addOption(
                null,
                "clear",
                true,
                "Clear all records in the database for this benchmark");
        cmdlineOptions.addOption(
                null,
                "load",
                true,
                "Load data using the benchmark's data loader");
        cmdlineOptions.addOption(
                null,
                "execute",
                true,
                "Execute the benchmark workload");
        cmdlineOptions.addOption(
                null,
                "runscript",
                true,
                "Run an SQL script");
        cmdlineOptions.addOption(
                null,
                "upload",
                true,
                "Upload the result");

        cmdlineOptions.addOption(null, "disable-stats", true, "Whether to disable stats collection");
        cmdlineOptions.addOption("v", "verbose", false, "Display Messages");
        cmdlineOptions.addOption("h", "help", false, "Print this help");
        cmdlineOptions.addOption("s", "sample", true, "Sampling window");
        cmdlineOptions.addOption("im", "interval-monitor", true, "Throughput Monitoring Interval in milliseconds");
        cmdlineOptions.addOption("ss", false, "Verbose Sampling per Transaction");
        cmdlineOptions.addOption("o", "output", true, "Output file (default System.out)");
        cmdlineOptions.addOption("d", "directory", true, "Base directory for the result files, default is current directory");
        cmdlineOptions.addOption("t", "timestamp", false, "Each result file is prepended with a timestamp for the beginning of the experiment");
        cmdlineOptions.addOption("ts", "tracescript", true, "Script of transactions to execute");
        cmdlineOptions.addOption(null, "histograms", false, "Print txn histograms");
        cmdlineOptions.addOption(null, "dialects-export", true, "Export benchmark SQL to a dialects file");
        cmdlineOptions.addOption(null, "output-raw", true, "Output raw data");
        cmdlineOptions.addOption(null, "output-samples", true, "Output sample data");


        // parse the command line arguments
        CommandLine argsLine = parser.parse(cmdlineOptions, args);
        if (argsLine.hasOption("h")) {
            printUsage(cmdlineOptions);
            return;
        } else if (argsLine.hasOption("c") == false) {
            LOG.error("Missing Configuration file");
            printUsage(cmdlineOptions);
            return;
        } else if (argsLine.hasOption("b") == false) {
            LOG.fatal("Missing Benchmark Class to load");
            printUsage(cmdlineOptions);
            return;
        }

        // Interval Monitor
        if (argsLine.hasOption("im")) {
            options.intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }
        // Enable/Disable Stats Collection
        if (argsLine.hasOption("disable-stats")) {
            options.collectStats = false;
        }

        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------
        
        options.targetBenchmarks = argsLine.getOptionValue("b").split(",");
        List<BenchmarkModule> benchList = new ArrayList<BenchmarkModule>();
        
        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<TransactionType>();
        
        options.configFile = argsLine.getOptionValue("c");

        // Export StatementDialects
        if (isBooleanOptionSet(argsLine, "dialects-export")) {
            BenchmarkModule bench = benchList.get(0);
            if (bench.getStatementDialects() != null) {
                LOG.info("Exporting StatementDialects for " + bench);
                String xml = bench.getStatementDialects().export(bench.getWorkloadConfiguration().getDBType(),
                                                                 bench.getProcedures().values());
                System.out.println(xml);
                System.exit(0);
            }
            throw new RuntimeException("No StatementDialects is available for " + bench);
        }

        
        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Creating new " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                runCreator(benchmark);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
                for (BenchmarkModule benchmark : benchList) {
                LOG.info("Resetting " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                benchmark.clearDatabase();
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping creating benchmark database tables");
            LOG.info(SINGLE_LINE);
        }

        // Execute Loader
        if (isBooleanOptionSet(argsLine, "load")) {
            for (BenchmarkModule benchmark : benchList) {
                LOG.info("Loading data into " + benchmark.getBenchmarkName().toUpperCase() + " database...");
                runLoader(benchmark);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("Skipping loading benchmark database records");
            LOG.info(SINGLE_LINE);
        }
        
        // Execute a Script
        if (argsLine.hasOption("runscript")) {
            for (BenchmarkModule benchmark : benchList) {
                String script = argsLine.getOptionValue("runscript");
                LOG.info("Running a SQL script: "+script);
                runScript(benchmark, script);
                LOG.info("Finished!");
                LOG.info(SINGLE_LINE);
            }
        }

        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            Results r = null;
            try {
                r = runWorkload(benchList, intervalMonitor, collectStats);
            } catch (Throwable ex) {
                LOG.error("Unexpected error when running benchmarks.", ex);
                System.exit(1);
            }
            assert(r != null);

            // WRITE OUTPUT
            writeOutputs(r, activeTXTypes, argsLine, xmlConfig);
            
            // WRITE HISTOGRAMS
            if (argsLine.hasOption("histograms")) {
                writeHistograms(r);
            }


        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }
    
    /**
     * Write out the results for a benchmark run to a bunch of files
     * @param r
     * @param activeTXTypes
     * @param argsLine
     * @param xmlConfig
     * @throws Exception
     */
    private static void writeOutputs(Results r, List<TransactionType> activeTXTypes, CommandLine argsLine, XMLConfiguration xmlConfig) throws Exception {
        
        // If an output directory is used, store the information
        String outputDirectory = "results";
        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }
        String filePrefix = "";
        if (argsLine.hasOption("t")) {
            filePrefix = String.valueOf(TimeUtil.getCurrentTime().getTime()) + "_";
        }
        
        // Special result uploader
        ResultUploader ru = null;
        if (xmlConfig.containsKey("uploadUrl")) {
            ru = new ResultUploader(r, xmlConfig, argsLine);
            LOG.info("Upload Results URL: " + ru);
        }
        
        // Output target 
        PrintStream ps = null;
        PrintStream rs = null;
        String baseFileName = "oltpbench";
        if (argsLine.hasOption("o")) {
            if (argsLine.getOptionValue("o") == "-") {
                ps = System.out;
                rs = System.out;
                baseFileName = null;
            } else {
                baseFileName = argsLine.getOptionValue("o");
            }
        }

        // Build the complex path
        String baseFile = filePrefix;
        String nextName;
        
        if (baseFileName != null) {
            // Check if directory needs to be created
            if (outputDirectory.length() > 0) {
                FileUtil.makeDirIfNotExists(outputDirectory.split("/"));
            }
            
            baseFile = filePrefix + baseFileName;

            if (argsLine.getOptionValue("output-raw", "true").equalsIgnoreCase("true")) {
                // RAW OUTPUT
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".csv"));
                rs = new PrintStream(new File(nextName));
                LOG.info("Output Raw data into file: " + nextName);
                r.writeAllCSVAbsoluteTiming(activeTXTypes, rs);
                rs.close();
            }

            if (isBooleanOptionSet(argsLine, "output-samples")) {
                // Write samples using 1 second window
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".samples"));
                rs = new PrintStream(new File(nextName));
                LOG.info("Output samples into file: " + nextName);
                r.writeCSV2(rs);
                rs.close();
            }

            // Result Uploader Files
            if (ru != null) {
                // Summary Data
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".summary"));
                PrintStream ss = new PrintStream(new File(nextName));
                LOG.info("Output summary data into file: " + nextName);
                ru.writeSummary(ss);
                ss.close();

                // DBMS Parameters
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".params"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output DBMS parameters into file: " + nextName);
                ru.writeDBParameters(ss);
                ss.close();

                // DBMS Metrics
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".metrics"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output DBMS metrics into file: " + nextName);
                ru.writeDBMetrics(ss);
                ss.close();

                // Experiment Configuration
                nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".expconfig"));
                ss = new PrintStream(new File(nextName));
                LOG.info("Output experiment config into file: " + nextName);
                ru.writeBenchmarkConf(ss);
                ss.close();
            }
            
        } else if (LOG.isDebugEnabled()) {
            LOG.debug("No output file specified");
        }
        
        if (isBooleanOptionSet(argsLine, "upload") && ru != null) {
            ru.uploadResult(activeTXTypes);
        }
        
        // SUMMARY FILE
        if (argsLine.hasOption("s")) {
            nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));
            ps = new PrintStream(new File(nextName));
            LOG.info("Output into file: " + nextName);
            
            int windowSize = Integer.parseInt(argsLine.getOptionValue("s"));
            LOG.info("Grouped into Buckets of " + windowSize + " seconds");
            r.writeCSV(windowSize, ps);

            // Allow more detailed reporting by transaction to make it easier to check
            if (argsLine.hasOption("ss")) {
                
                for (TransactionType t : activeTXTypes) {
                    PrintStream ts = ps;
                    if (ts != System.out) {
                        // Get the actual filename for the output
                        baseFile = filePrefix + baseFileName + "_" + t.getName();
                        nextName = FileUtil.getNextFilename(FileUtil.joinPath(outputDirectory, baseFile + ".res"));                            
                        ts = new PrintStream(new File(nextName));
                        r.writeCSV(windowSize, ts, t);
                        ts.close();
                    }
                }
            }
        } else if (LOG.isDebugEnabled()) {
            LOG.warn("No bucket size specified");
        }
        
        if (ps != null) ps.close();
        if (rs != null) rs.close();
    }

    private static void printUsage(Options options) {
        HelpFormatter hlpfrmt = new HelpFormatter();
        hlpfrmt.printHelp("oltpbenchmark", options);
    }

    /**
     * Returns true if the given key is in the CommandLine object and is set to
     * true.
     * 
     * @param argsLine
     * @param key
     * @return
     */
    private static boolean isBooleanOptionSet(CommandLine argsLine, String key) {
        if (argsLine.hasOption(key)) {
            LOG.debug("CommandLine has option '" + key + "'. Checking whether set to true");
            String val = argsLine.getOptionValue(key);
            LOG.debug(String.format("CommandLine %s => %s", key, val));
            return (val != null ? val.equalsIgnoreCase("true") : false);
        }
        return (false);
    }
    
    public static String getAssertWarning() {
        String msg = "!!! WARNING !!!\n" +
                     "OLTP-Bench is executing with JVM asserts enabled. This will degrade runtime performance.\n" +
                     "You can disable them by setting the config option 'assertions' to FALSE";
        return StringBoxUtil.heavyBox(msg);
    }
}
