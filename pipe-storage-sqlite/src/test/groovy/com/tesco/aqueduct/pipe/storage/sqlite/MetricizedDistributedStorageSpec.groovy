package com.tesco.aqueduct.pipe.storage.sqlite


import com.tesco.aqueduct.pipe.api.OffsetEntity
import com.tesco.aqueduct.pipe.api.OffsetName
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import spock.lang.Specification
import spock.lang.Unroll

class MetricizedDistributedStorageSpec extends Specification {

    private MetricizedDistributedStorage metricizedDistributedStorage
    def timedDistributedStorage = Mock(TimedDistributedStorage)
    def meterRegistry = new SimpleMeterRegistry()

    @Unroll
    def "write offset is recorded as gauge metric"() {
        given: "a metricized distributed storage and an offset entity"
        metricizedDistributedStorage = new MetricizedDistributedStorage(timedDistributedStorage, meterRegistry)
        def offsetEntity = new OffsetEntity(offsetName, OptionalLong.of(offsetValue))

        when: "an offset is written"
        metricizedDistributedStorage.write(offsetEntity)

        then: "the offset metric is recorded and the timed distributed storage is invoked"
        meterRegistry.find(metricName)?.meter()?.measure()?.first()?.value == offsetValue
        1 * timedDistributedStorage.write(offsetEntity)

        where:
        metricName                          | offsetName                        | offsetValue
        "pipe.offset.globalLatestOffset"    | OffsetName.GLOBAL_LATEST_OFFSET   | 100L
        "pipe.offset.localLatestOffset"     | OffsetName.LOCAL_LATEST_OFFSET    | 15L
        "pipe.offset.pipeOffset"            | OffsetName.PIPE_OFFSET            | 54465L
    }
}
