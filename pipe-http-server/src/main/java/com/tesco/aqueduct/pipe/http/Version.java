package com.tesco.aqueduct.pipe.http;

public class Version {

    private static final String DEFAULT_VERSION = "0.0.0";

    public static String getImplementationVersion() {
        String version = Version.class.getPackage().getImplementationVersion();
        return version == null ? DEFAULT_VERSION : version;
    }
}
