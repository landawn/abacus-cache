package com.landawn.abacus;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

/** Captures one class's diagnostics and restores its exact logger configuration on close. */
public final class LogCapture implements AutoCloseable {
    public static final String RESOURCE = "log4j2-configuration";

    private final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    private final Configuration configuration = context.getConfiguration();
    private final String loggerName;
    private final LoggerConfig previous;
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();
    private final AbstractAppender appender;

    public LogCapture(final Class<?> type, final Level level) {
        loggerName = type.getName();
        previous = configuration.getLoggers().get(loggerName);
        appender = new AbstractAppender("capture-" + loggerName, null, PatternLayout.createDefaultLayout(), false, Property.EMPTY_ARRAY) {
            @Override
            public void append(final LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        final LoggerConfig logger = new LoggerConfig(loggerName, level, false);
        logger.addAppender(appender, level, null);
        configuration.addLogger(loggerName, logger);
        context.updateLoggers();
    }

    public List<String> messages() {
        return events.stream().map(event -> event.getMessage().getFormattedMessage()).toList();
    }

    @Override
    public void close() {
        configuration.removeLogger(loggerName);
        if (previous != null) {
            configuration.addLogger(loggerName, previous);
        }
        context.updateLoggers();
        appender.stop();
    }
}
