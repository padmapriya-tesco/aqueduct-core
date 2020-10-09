package com.tesco.aqueduct.pipe.storage

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource

class PostgresqlStorageSpec extends Specification {

    @Shared
    def retryAfter = 30

    @Unroll
    def "retry after is #result when queryTime is #timeOfQueryMs and message result size is #noOfMessages"() {
        given:
        def readersNodeCount = 3000
        def clusterDBPoolSize = 60

        and:
        def storage = new PostgresqlStorage(Mock(DataSource), 20, retryAfter, 2, 0, readersNodeCount, clusterDBPoolSize, 4)

        expect:
        if (shouldBeGreaterThan) {
            storage.calculateRetryAfter(timeOfQueryMs, noOfMessages) > result
        } else {
            storage.calculateRetryAfter(timeOfQueryMs, noOfMessages) == result
        }

        where:
        timeOfQueryMs | noOfMessages | result     | shouldBeGreaterThan
        100           | 0            | retryAfter | true
        1234          | 0            | retryAfter | true
        100           | 10000        | 5          | false
        200           | 10000        | 10         | false
        10            | 10000        | 1          | false
        50            | 10000        | 3          | false
        0             | 10000        | 1          | false
        1             | 10000        | 1          | false
        700           | 10000        | retryAfter | true
        1000          | 10000        | retryAfter | true
    }
}
