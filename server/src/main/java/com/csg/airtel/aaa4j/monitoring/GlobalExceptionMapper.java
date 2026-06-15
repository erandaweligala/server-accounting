package com.csg.airtel.aaa4j.monitoring;

import com.csg.airtel.aaa4j.common.util.LoggingUtil;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import com.csg.airtel.aaa4j.exception.BaseException;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.jboss.logging.Logger;

/**
 * Global JAX-RS exception mapper. Catches any Throwable escaping a REST endpoint
 * and feeds it through {@link ErrorMonitoringService} so uncaught failures in the
 * controller layer are counted and logged centrally.
 *
 * Per-endpoint Mutiny recovery (e.g. CoAResource) still runs first; this mapper
 * is the safety net for anything that slips past those handlers.
 */
@Provider
public class GlobalExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger logger = Logger.getLogger(GlobalExceptionMapper.class);
    private static final String CLASS_NAME = "GlobalExceptionMapper";
    private static final String INTERNAL_ERROR_JSON = "{\"error\":\"Internal server error\"}";

    @Inject
    ErrorMonitoringService errorMonitoringService;

    @Inject
    ExceptionMetricsService exceptionMetrics;

    @Override
    public Response toResponse(Throwable throwable) {
        if (throwable instanceof BaseException baseException) {
            errorMonitoringService.recordError(
                    ErrorCategory.REST_UNHANDLED_EXCEPTION,
                    CLASS_NAME,
                    "toResponse",
                    baseException);
            exceptionMetrics.recordException(baseException,
                    ExceptionMetricsService.Layer.RESOURCE,
                    ExceptionMetricsService.Source.INTERNAL);
            return Response.status(baseException.getHttpStatus())
                    .entity(baseException.getMessage())
                    .build();
        }

        errorMonitoringService.recordRestUnhandled(CLASS_NAME, "toResponse", throwable);
        exceptionMetrics.recordException(throwable,
                ExceptionMetricsService.Layer.RESOURCE,
                ExceptionMetricsService.Source.INTERNAL);
        LoggingUtil.logError(logger, CLASS_NAME, "toResponse", throwable,
                "Unhandled exception escaped REST layer");

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(INTERNAL_ERROR_JSON)
                .build();
    }
}
