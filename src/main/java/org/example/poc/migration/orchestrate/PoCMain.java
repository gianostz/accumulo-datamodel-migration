package org.example.poc.migration.orchestrate;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PoCMain {

    private static final Logger log = LoggerFactory.getLogger(PoCMain.class);

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        log.info("PoC starting. dataset.totalEvents={}, waves.count={}, spark.master={}",
                config.getInt("dataset.totalEvents"),
                config.getInt("waves.count"),
                config.getString("spark.master"));
        log.warn("WaveOrchestrator not yet implemented. See docs/03-architecture.md §2.8.");
    }

    private PoCMain() {}
}
