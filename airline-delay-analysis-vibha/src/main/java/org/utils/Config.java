package org.utils;

import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Properties props = new Properties();

    static {
        try (InputStream input = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Config file not found in resources");
            }
            props.load(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration: " + e.getMessage(), e);
        }
    }

    // Generic key fetch (env aware)
    public static String get(String key) {
        String env = props.getProperty(Constants.ENV, "local");
        return props.getProperty(env + "." + key);
    }

    // Schema fetch
    public static String getSchema(String layer) {
        return props.getProperty(layer.toLowerCase() + ".schema");
    }

    // Path fetch
    public static String getOutputPath(String layer) {
        String env = props.getProperty(Constants.ENV, "local");
        return props.getProperty(env + "." + layer.toLowerCase() + ".path");
    }

    // DB URL (schema aware)
    public static String getDbUrl(String layer) {
        return get(Constants.DB_URL) + getSchema(layer);
    }

    public static String getDbUser() {
        return get(Constants.DB_USER);
    }

    public static String getDbPassword() {
        return get(Constants.DB_PASS);
    }

    public static String getDbDriver() {
        return get(Constants.DB_DRIVER);
    }
}
