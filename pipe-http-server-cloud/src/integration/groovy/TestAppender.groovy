import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase

import java.util.concurrent.CopyOnWriteArrayList

class TestAppender extends AppenderBase<ILoggingEvent> {
    static List<ILoggingEvent> events = new CopyOnWriteArrayList<>()

    @Override
    void append(ILoggingEvent e) {
        events.add(e)
    }

    static List<ILoggingEvent> getEvents() {
        return events
    }

    static void clearEvents() {
        events.clear()
    }
}