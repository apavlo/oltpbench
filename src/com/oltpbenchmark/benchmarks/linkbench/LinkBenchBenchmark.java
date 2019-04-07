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

package com.oltpbenchmark.benchmarks.linkbench;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.linkbench.procedures.AddNode;
import com.oltpbenchmark.benchmarks.linkbench.utils.ConfigUtil;


public class LinkBenchBenchmark extends BenchmarkModule {

    private static final Logger LOG = Logger.getLogger(LinkBenchBenchmark.class);
    private LinkBenchConfiguration linkBenchConf;
    private Properties props;

    public LinkBenchBenchmark(WorkloadConfiguration workConf) throws Exception {
        super("linkbench", workConf, true);
        this.linkBenchConf = new LinkBenchConfiguration(workConf);
        props = new Properties();
        props.load(new FileInputStream(this.linkBenchConf.getConfigFile()));
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
        Random masterRandom = createMasterRNG(props, LinkBenchConstants.REQUEST_RANDOM_SEED);
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new LinkBenchWorker(this, i, new Random(masterRandom.nextLong()), props, workConf.getTerminals()));
        } // FOR
        return workers;
    }

    @Override
    protected Loader<LinkBenchBenchmark> makeLoaderImpl(Connection conn) throws SQLException {
        return new LinkBenchLoader(this, conn);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return AddNode.class.getPackage();
    }

    /**
     * Create a new random number generated, optionally seeded to a known
     * value from the config file.  If seed value not provided, a seed
     * is chosen.  In either case the seed is logged for later reproducibility.
     * @param props
     * @param configKey config key for the seed value
     * @return
     */
    private Random createMasterRNG(Properties props, String configKey) {
        long seed;
        if (props.containsKey(configKey)) {
            seed = ConfigUtil.getLong(props, configKey);
            LOG.info("Using configured random seed " + configKey + "=" + seed);
        } else {
            seed = System.nanoTime() ^ (long)configKey.hashCode();
            LOG.info("Using random seed " + seed + " since " + configKey
                    + " not specified");
        }

        SecureRandom masterRandom;
        try {
            masterRandom = SecureRandom.getInstance("SHA1PRNG");
        } catch (NoSuchAlgorithmException e) {
            LOG.warn("SHA1PRNG not available, defaulting to default SecureRandom" +
            " implementation");
            masterRandom = new SecureRandom();
        }
        masterRandom.setSeed(ByteBuffer.allocate(8).putLong(seed).array());

        // Can be used to check that rng is behaving as expected
        LOG.info("First number generated by master " + configKey +
                ": " + masterRandom.nextLong());
        return masterRandom;
    }
}
