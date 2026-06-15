package com.csg.airtel.aaa4j.monitoring;

import com.csg.airtel.aaa4j.common.util.LoggingUtil;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

/**
 * Centralized error monitoring service.

 * Every recorded error is:
 *   1. Counted in Micrometer (exported on /q/metrics for Prometheus).
 *   2. Logged with structured context so Grafana / log search can correlate.

 * Use {@link #recordError(ErrorCategory, String, String, Throwable)} for general
 * tracking, and the specialized helpers (HTTP / Kafka / UDP) for common cases.
 */
@ApplicationScoped
public class ErrorMonitoringService {

    private static final Logger logger = Logger.getLogger(ErrorMonitoringService.class);
    private static final String CLASS_NAME = "ErrorMonitoringService";

    private static final String ERROR_COUNTER = "radius.server.errors";
    private static final String ERROR_COUNTER_DESCRIPTION =
            "Total errors observed in the RADIUS server, tagged by category, component and exception type";

    private static final String TAG_CATEGORY = "category";
    private static final String TAG_COMPONENT = "component";
    private static final String TAG_OPERATION = "operation";
    private static final String TAG_EXCEPTION = "exception";

    private static final String UNKNOWN = "unknown";

    private final MeterRegistry meterRegistry;

    @Inject
    public ErrorMonitoringService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Record an error and increment the Micrometer counter.
     *
     * @param category   high-level error bucket (HTTP / Kafka / UDP / uncaught ...)
     * @param component  the component that observed the error (e.g. class name)
     * @param operation  the logical operation that failed (e.g. method name)
     * @param throwable  the underlying error, may be null
     */
    public void recordError(ErrorCategory category, String component, String operation, Throwable throwable) {
        String exceptionName = throwable != null ? throwable.getClass().getSimpleName() : "none";

        Counter.builder(ERROR_COUNTER)
                .description(ERROR_COUNTER_DESCRIPTION)
                .tags(Tags.of(
                        TAG_CATEGORY, category.tagValue(),
                        TAG_COMPONENT, safe(component),
                        TAG_OPERATION, safe(operation),
                        TAG_EXCEPTION, exceptionName))
                .register(meterRegistry)
                .increment();

        LoggingUtil.logError(logger, CLASS_NAME, "recordError", throwable,
                "ERROR_MONITOR category=%s component=%s operation=%s exception=%s message=%s",
                category.tagValue(), safe(component), safe(operation), exceptionName,
                throwable != null ? throwable.getMessage() : "n/a");
    }

    /**
     * HTTP-specific helper that auto-classifies the failure as either a
     * timeout or a generic HTTP failure based on the throwable.
     */
    public void recordHttpFailure(String component, String operation, Throwable throwable) {
        ErrorCategory category = isTimeout(throwable) ? ErrorCategory.HTTP_TIMEOUT : ErrorCategory.HTTP_FAILURE;
        recordError(category, component, operation, throwable);
    }

    /**
     * Kafka-specific helper. {@link org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException}
     * and {@link java.util.concurrent.TimeoutException} are both tagged as KAFKA_TIMEOUT.
     */
    public void recordKafkaFailure(String component, String operation, Throwable throwable) {
        ErrorCategory category = isTimeout(throwable) ? ErrorCategory.KAFKA_TIMEOUT : ErrorCategory.KAFKA_FAILURE;
        recordError(category, component, operation, throwable);
    }

    public void recordKafkaCircuitOpen(String component, String operation, Throwable throwable) {
        recordError(ErrorCategory.KAFKA_CIRCUIT_OPEN, component, operation, throwable);
    }

    /**
     * UDP-specific helper. {@link SocketTimeoutException} is tagged as UDP_TIMEOUT,
     * everything else as UDP_FAILURE.
     */
    public void recordUdpFailure(String component, String operation, Throwable throwable) {
        ErrorCategory category = (throwable instanceof SocketTimeoutException)
                ? ErrorCategory.UDP_TIMEOUT
                : ErrorCategory.UDP_FAILURE;
        recordError(category, component, operation, throwable);
    }

    public void recordProgrammingException(String component, String operation, Throwable throwable) {
        recordError(ErrorCategory.PROGRAMMING_EXCEPTION, component, operation, throwable);
    }

    public void recordUncaughtException(String component, String operation, Throwable throwable) {
        recordError(ErrorCategory.UNCAUGHT_EXCEPTION, component, operation, throwable);
    }

    public void recordRestUnhandled(String component, String operation, Throwable throwable) {
        recordError(ErrorCategory.REST_UNHANDLED_EXCEPTION, component, operation, throwable);
    }

    private static boolean isTimeout(Throwable t) {
        Throwable cursor = t;
        while (cursor != null) {
            if (cursor instanceof TimeoutException
                    || cursor instanceof org.eclipse.microprofile.faulttolerance.exceptions.TimeoutException
                    || cursor instanceof SocketTimeoutException) {
                return true;
            }
            if (cursor.getCause() == cursor) {
                break;
            }
            cursor = cursor.getCause();
        }
        return false;
    }

    private static String safe(String value) {
        return (value == null || value.isBlank()) ? UNKNOWN : value;
    }
}
