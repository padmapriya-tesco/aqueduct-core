import com.tesco.aqueduct.pipe.api.Message
import com.tesco.aqueduct.pipe.logger.PipeLogger
import org.slf4j.Logger
import org.slf4j.MDC
import spock.lang.Specification

import java.time.ZoneOffset
import java.time.ZonedDateTime

class PipeLoggerSpec extends Specification {

    static ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC)

    def "Logger logs as expected with message"() {
        given:
        Logger logger = Mock()
        logger.isDebugEnabled() >> true
        PipeLogger LOG = new PipeLogger(logger)
        Message message = new Message("type", "key", "contentType",0 , time, "data")

        when:
        LOG.withMessage(message).debug("testWhere", "testWhat")

        then:
        1 * logger.debug("testWhat")
    }

    def "Logger doesn't log debug messages when debug is disabled"() {
        given:
        Logger logger = Mock()
        logger.isDebugEnabled() >> false
        PipeLogger LOG = new PipeLogger(logger)
        Message message = new Message("type", "key", "contentType",0, time, "data")

        when:
        LOG.withMessage(message).debug("testWhere", "testWhat")

        then:
        0 * logger.debug("testWhat")
    }

    def "Logging errors"() {
        given:
        Logger logger = Mock()

        PipeLogger LOG = new PipeLogger(logger)
        Message message = new Message("type", "key", "contentType",0, time, "data")

        when:
        LOG.withMessage(message).error("testWhere", "testWhat", "testWhy")

        then:
        true
        1 * logger.error("testWhat", "testWhy")
    }

    def "Logging fields without a message"() {
        given:
        Logger logger = Mock()

        PipeLogger LOG = new PipeLogger(logger)

        when:
        LOG.error("testWhere", "testWhat", "testWhy")

        then:
        true
        1 * logger.error("testWhat", "testWhy")
    }

    def "Logging with fields that are allowed to be null"() {
        given:
        Logger logger = Mock()

        PipeLogger LOG = new PipeLogger(logger)
        Message message = new Message("type", "key", "contentType",0, time, "data")

        when:
        LOG.withMessage(message).error("testWhere", "testWhat", "testWhy")

        then:
        true
        1 * logger.error("testWhat", "testWhy")
    }
    def "Logging preserves MDC state as before log invocation and clears MDC state generated for this log"() {
        given:
        Logger logger = Mock()
        logger.isInfoEnabled() >> true

        PipeLogger LOG = new PipeLogger(logger)
        Message message = new Message("type", "key", "contentType",0, time, "data")

        and: "MDC has a state before logging"
        MDC.put("trace_id", "someTraceId")

        when:
        LOG.withMessage(message).info("testWhere", "testWhat")

        then: "MDC state is preserved as before"
        MDC.getCopyOfContextMap() == ["trace_id" : "someTraceId"]

        and: "info logging invoked"
        1 * logger.info("testWhat")
    }
}