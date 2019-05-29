package com.tesco.aqueduct.pipe.http

import spock.lang.Specification

class VersionSpec extends Specification {

    // This is properly tested in system tests
    def "Version is available and follows specific format" () {
        expect:
        Version.getImplementationVersion() ==~ /\d\.\d+\.\d+(-SNAPSHOT)?/
    }

    def "During unit tests there is no jar so we will get default value" () {
        expect:
        Version.getImplementationVersion() == "0.0.0"
    }
}
