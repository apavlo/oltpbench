package com.oltpbenchmark;

import org.apache.commons.configuration.XMLConfiguration;

/**
 * Configuration Options for OLTP-Bench
 */
public class OLTPBenchOptions {

    /**
     *
     */
    public XMLConfiguration pluginConfig = null;

    /**
     *
     */
    public String[] targetBenchmarks;

    /**
     *
     */
    public String configFile = null;


    // -------------------------------------------------------------------
    // ACTION OPTIONS
    // -------------------------------------------------------------------

    public boolean createDatabase = false;

    public boolean resetDatabase = false;

    public boolean loadDatabase = false;

    public boolean executeWorkload = false;

    /**
     *
     */
    public boolean collectStats = true;

    // -------------------------------------------------------------------
    // CONFIGURATION OPTIONS
    // -------------------------------------------------------------------

    /**
     * Interval Monitor (Seconds)
     */
    public int intervalMonitor = 0;

    /**
     *
     */
    public String outputDirectory = "results";

    /**
     *
     */
    public String outputFilePrefix = "";


    /**
     *
     */
    public String outputFileBaseName = "oltpbench";


    /**
     *
     */
    public boolean outputSummaryFile = false;

    /**
     *
     */
    public int outputWindowSize = 0;

    /**
     *
     */
    public String uploadURL = "";

    /**
     *
     */
    public boolean uploadEnable = false;


    /**
     *
     */
    public boolean dialectsExport = false;


}
