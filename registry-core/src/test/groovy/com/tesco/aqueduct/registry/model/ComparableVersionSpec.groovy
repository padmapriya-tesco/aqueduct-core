package com.tesco.aqueduct.registry.model

import spock.lang.Specification

class ComparableVersionSpec extends Specification {
    def "node version constructor"() {
        given: "a string containing a semantic version"
        String version = "0.5.1"

        when: "a node version is constructed"
        ComparableVersion comparableVersion = new ComparableVersion(version)

        then: "the node version is constructed correctly and behaves as expected"
        comparableVersion.equals(new ComparableVersion(version))
        comparableVersion.compareTo(new ComparableVersion("0.5.1")) == 0
        comparableVersion.compareTo(new ComparableVersion("0.5.0")) == 1
        comparableVersion.compareTo(new ComparableVersion("0.5.2")) == -1
        comparableVersion.compareTo(new ComparableVersion("0.6.0")) == -1
        comparableVersion.compareTo(new ComparableVersion("1.5.0")) == -1
        comparableVersion.compareTo(new ComparableVersion("abc")) == 1
        comparableVersion.compareTo(new ComparableVersion("")) == 1
    }

    def "null version"() {
        given: "a string containing a semantic version"
        String version = null

        when: "a node version is constructed"
        ComparableVersion comparableVersion = new ComparableVersion(version)

        then: "the node version is constructed correctly and behaves as expected"
        thrown(NullPointerException)
    }
}
