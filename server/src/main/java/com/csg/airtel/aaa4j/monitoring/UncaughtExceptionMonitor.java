package com.csg.airtel.aaa4j.monitoring;

import com.csg.airtel.aaa4j.common.util.LoggingUtil;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Registers a JVM-wide default uncaught exception handler so that any
 * exception escaping a thread (event loop, worker pool, custom threads,
 * scheduled tasks) is counted and logged via {@link ErrorMonitoringService}.
 *
 * Without this, errors that escape Vert.x worker threads or background
 * threads typically die silently with only a stderr dump.
 */
@ApplicationScoped
public class UncaughtExceptionMonitor {

    private static final Logger logger = Logger.getLogger(UncaughtExceptionMonitor.class);
    private static final String CLASS_NAME = "UncaughtExceptionMonitor";

    @Inject
    ErrorMonitoringService errorMonitoringService;

    @Inject
    ExceptionMetricsService exceptionMetrics;

    void onStart(@Observes StartupEvent event) {
        Thread.UncaughtExceptionHandler existing = Thread.getDefaultUncaughtExceptionHandler();

        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> {
            try {
                errorMonitoringService.recordUncaughtException(
                        CLASS_NAME,
                        thread != null ? thread.getName() : "unknown-thread",
                        throwable);
                exceptionMetrics.recordException(throwable,
                        ExceptionMetricsService.Layer.SERVICE,
                        ExceptionMetricsService.Source.INTERNAL);
            } catch (Throwable monitoringError) {
                LoggingUtil.logError(logger, CLASS_NAME, "uncaughtHandler", monitoringError,
                        "Failed to record uncaught exception metric");
            }

            if (existing != null) {
                try {
                    existing.uncaughtException(thread, throwable);
                } catch (Throwable chainedError) {
                    LoggingUtil.logError(logger, CLASS_NAME, "uncaughtHandler", chainedError,
                            "Previously installed uncaught handler failed");
                }
            }
        });

        LoggingUtil.logInfo(logger, CLASS_NAME, "onStart",
                "Default uncaught exception handler installed");
    }
}
