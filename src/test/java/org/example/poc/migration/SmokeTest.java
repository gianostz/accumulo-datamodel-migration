package org.example.poc.migration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SmokeTest {

    @Test
    void applicationConfLoadsWithExpectedKeys() {
        Config config = ConfigFactory.load();
        assertTrue(config.getInt("dataset.totalEvents") > 0);
        assertEquals(2, config.getInt("waves.count"));
        assertEquals("local[*]", config.getString("spark.master"));
    }
}
