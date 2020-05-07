package com.tesco.aqueduct.pipe.http;

public class Version {

    private static final String DEFAULT_VERSION = "0.0.0";

    public static void main(String[] args) {
        System.out.println(Version.getImplementationVersion());
    }

    public static String getImplementationVersion() {
        final String version = Version.class.getPackage().getImplementationVersion();
        return version == null ? DEFAULT_VERSION : version;
    }
}
