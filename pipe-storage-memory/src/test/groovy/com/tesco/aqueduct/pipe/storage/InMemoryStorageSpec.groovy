package com.tesco.aqueduct.pipe.storage

import com.tesco.aqueduct.pipe.api.Message
import spock.lang.Unroll


class InMemoryStorageSpec extends StorageSpec {

    InMemoryStorage storage = new CentralInMemoryStorage(limit, 10000)

    @Override
    void insert(Message msg) {
        storage.write(msg)
    }

    @Unroll
    def "messages have to be put in order even if they replacing existing message"() {
        when:
        insert(message(offset:1, key: "x"))
        insert(message(offset:2, key: "y"))
        insert(message(offset:1, key: "x"))

        then:
        thrown(IllegalStateException)
    }

    @Unroll
    def "messages have to be put in order"() {
        when:
        insert(message(offset: 123, key:"y"))
        insert(message(offset: 122, key:"x"))

        then:
        thrown(IllegalStateException)
    }
}
