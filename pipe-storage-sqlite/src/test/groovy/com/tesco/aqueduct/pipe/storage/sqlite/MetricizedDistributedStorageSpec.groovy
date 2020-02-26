package com.tesco.aqueduct.pipe.storage.sqlite

import com.tesco.aqueduct.pipe.api.OffsetEntity
import com.tesco.aqueduct.pipe.api.OffsetName
import io.micrometer.core.instrument.MeterRegistry
import spock.lang.Specification

import java.util.concurrent.atomic.AtomicLong

class MetricizedDistributedStorageSpec extends Specification {

    private MetricizedDistributedStorage metricizedDistributedStorage
    def timedDistributedStorage = Mock(TimedDistributedStorage)
    def meterRegistry = Mock(MeterRegistry)

    void setup() {
        metricizedDistributedStorage = new MetricizedDistributedStorage(timedDistributedStorage, meterRegistry)
    }

    def "write offset is recorded as gauge metric"() {
        given: "meter registry with guage registered for global latest offset"
        def atomicLongForGlobalOffset = new AtomicLong(0)
        meterRegistry.gauge(OffsetName.GLOBAL_LATEST_OFFSET.toString(), atomicLongForGlobalOffset) >> atomicLongForGlobalOffset

        when: "global latest offset is written"
        metricizedDistributedStorage.write(new OffsetEntity(OffsetName.GLOBAL_LATEST_OFFSET, OptionalLong.of(100)))

        then: "atomic global offset registered with meter registry is updated with the value"
        atomicLongForGlobalOffset.get() == 100l
    }
}
