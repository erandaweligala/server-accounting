package com.csg.airtel.aaa4j.domain.producer;

import com.csg.airtel.aaa4j.common.constant.AuthServiceConstants;
import com.csg.airtel.aaa4j.common.util.LoggingUtil;
import com.csg.airtel.aaa4j.domain.model.AccountingRequestDto;
import com.csg.airtel.aaa4j.domain.service.ExceptionMetricsService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Fallback;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;
import org.slf4j.MDC;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class RadiusAccountingProducer {

    private static final Logger logger = Logger.getLogger(RadiusAccountingProducer.class);
    private static final String CLASS_NAME = "RadiusAccountingProducer";

    private static final ThreadLocal<StringBuilder> PARTITION_KEY_BUILDER =
            ThreadLocal.withInitial(() -> new StringBuilder(128));
    public static final String PRODUCE_ACCOUNTING_EVENT = "produceAccountingEvent";

    private final Emitter<AccountingRequestDto> accountingEmitter;
    private final Counter fallbackCounter;
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private volatile boolean shuttingDown = false;
    private final ExceptionMetricsService exceptionMetrics;

    @Inject
    public RadiusAccountingProducer(
            @Channel("accounting-events") Emitter<AccountingRequestDto> accountingEmitter,
            MeterRegistry meterRegistry,
            ExceptionMetricsService exceptionMetrics) {
        this.accountingEmitter = accountingEmitter;
        this.fallbackCounter = meterRegistry.counter("accounting.publish.fallback");
        this.exceptionMetrics = exceptionMetrics;
        LoggingUtil.logDebug(logger, CLASS_NAME, "constructor",
                "RadiusAccountingProducer initialized");
    }

    @Timeout(3000) // 3 second timeout for Kafka publish operations
    @CircuitBreaker(
            requestVolumeThreshold = 10,
            failureRatio = 0.5,
            delay = 30000,
            successThreshold = 3
    )
    @Fallback(fallbackMethod = "fallbackProduceAccountingEvent")
    public CompletionStage<Void> produceAccountingEvent(AccountingRequestDto request) {
        if (shuttingDown) {
            LoggingUtil.logWarn(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT,
                    "Skipping Kafka publish during shutdown - sessionId: %s, action: %s",
                    request.sessionId(), request.actionType());
            return CompletableFuture.failedFuture(new EmitterUnavailableException("Producer is shutting down"));
        }
        try {
            long startTime = System.currentTimeMillis();
            String partitionKey = buildPartitionKey(request.sessionId(), request.nasIP());

            String traceId = MDC.get(AuthServiceConstants.TRACE_ID);
            String userName = MDC.get(AuthServiceConstants.PARAM_USER_NAME);
            String sessionId = MDC.get(AuthServiceConstants.SESSION_ID);

            LoggingUtil.logDebug(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT,
                    "Publishing %s event for sessionId: %s, partitionKey: %s",
                    request.actionType(), request.sessionId(), partitionKey);

            var metadata = OutgoingKafkaRecordMetadata.<String>builder()
                    .withKey(partitionKey)
                    .build();
            CompletableFuture<Void> future = new CompletableFuture<>();

            var message = Message.of(request)
                    .addMetadata(metadata)
                    .withAck(() -> {
                        future.complete(null);
                        long duration = System.currentTimeMillis() - startTime;
                        applyMdc(traceId, userName, sessionId);
                        try {
                            LoggingUtil.logInfo(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT,
                                    "Successfully sent %s event for  %d ms",
                                    request.actionType(), duration);
                        } finally {
                            MDC.clear();
                        }
                        return CompletableFuture.completedFuture(null);
                    })
                    .withNack(throwable -> {
                        long failures = consecutiveFailures.incrementAndGet();
                        applyMdc(traceId, userName, sessionId);
                        try {
                            LoggingUtil.logError(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT, throwable,
                                    "Failed to send %s event for sessionId: %s (consecutive failures: %d)",
                                    request.actionType(), request.sessionId(), failures);
                        } finally {
                            MDC.clear();
                        }
                        exceptionMetrics.recordException(throwable,
                                ExceptionMetricsService.Layer.PRODUCER,
                                ExceptionMetricsService.Source.KAFKA);
                        future.completeExceptionally(throwable);
                        return CompletableFuture.completedFuture(null);
                    });

            accountingEmitter.send(message);
            return future;

        } catch (Exception e) {
            long failures = consecutiveFailures.incrementAndGet();
            LoggingUtil.logError(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT, e,
                    "Error producing %s event for sessionId: %s (consecutive failures: %d)",
                    request.actionType(), request.sessionId(), failures);
            exceptionMetrics.recordException(e,
                    ExceptionMetricsService.Layer.PRODUCER,
                    ExceptionMetricsService.Source.KAFKA);
            return CompletableFuture.failedFuture(e);
        }

    }

    /**
     * Fire-and-forget variant that avoids CompletableFuture allocation overhead.
     * Use this when the caller does not need to track the outcome of the publish.
     * ACK/NACK callbacks still handle logging and metrics internally.

     @CircuitBreaker(
     requestVolumeThreshold = 10,
     failureRatio = 0.5,
     delay = 30000,
     successThreshold = 3
     )
     @Fallback(fallbackMethod = "fallbackFireAndForgetAccountingEvent")
     public void fireAndForgetAccountingEvent(AccountingRequestDto request) {
     try {
     long startTime = System.currentTimeMillis();
     String partitionKey = buildPartitionKey(request.sessionId(), request.nasIP());

     LoggingUtil.logDebug(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT,
     "Publishing %s event for sessionId: %s, partitionKey: %s",
     request.actionType(), request.sessionId(), partitionKey);

     var metadata = OutgoingKafkaRecordMetadata.<String>builder()
     .withKey(partitionKey)
     .build();

     var message = Message.of(request)
     .addMetadata(metadata)
     .withAck(() -> {
     consecutiveFailures.set(0);
     long duration = System.currentTimeMillis() - startTime;
     LoggingUtil.logInfo(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT,
     "Successfully sent %s event for  %d ms",
     request.actionType(), duration);
     return CompletableFuture.completedFuture(null);
     })
     .withNack(throwable -> {
     long failures = consecutiveFailures.incrementAndGet();
     failureCounter.increment();
     LoggingUtil.logError(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT, throwable,
     "Failed to send %s event for sessionId: %s (consecutive failures: %d)",
     request.actionType(), request.sessionId(), failures);
     return CompletableFuture.completedFuture(null);
     });

     accountingEmitter.send(message);

     } catch (Exception e) {
     long failures = consecutiveFailures.incrementAndGet();
     failureCounter.increment();
     LoggingUtil.logError(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT, e,
     "Error producing %s event for sessionId: %s (consecutive failures: %d)",
     request.actionType(), request.sessionId(), failures);
     }
     }
     */


    /**
     * Set the shutdown flag as soon as Quarkus starts the shutdown sequence, so any
     * accounting packets that race in after the SmallRye Reactive Messaging emitter
     * has been disconnected fail fast with a clear reason instead of throwing
     * SRMSG00019 "Unable to connect an emitter" deep inside the messaging stack.
     */
    void onShutdown(@Observes ShutdownEvent event) {
        shuttingDown = true;
        LoggingUtil.logInfo(logger, CLASS_NAME, "onShutdown",
                "Shutdown initiated - new accounting publishes will be rejected");
    }

    /**
     * Marker exception used when the producer rejects a publish because the
     * application is shutting down. Distinguishes shutdown-time skips from real
     * Kafka failures in the handler's catch block.
     */
    public static class EmitterUnavailableException extends RuntimeException {
        public EmitterUnavailableException(String message) {
            super(message);
        }
    }

    /**
     * Fallback for fire-and-forget variant when circuit breaker is open
     */
    public void fallbackFireAndForgetAccountingEvent(AccountingRequestDto request) {
        fallbackCounter.increment();
        long failures = consecutiveFailures.get();
        LoggingUtil.logError(logger, CLASS_NAME, PRODUCE_ACCOUNTING_EVENT, null,
                "Circuit breaker activated - Fallback for %s event - SessionId: %s, NasIP: %s (consecutive failures: %d)",
                request.actionType(), request.sessionId(), failures);
    }

    /**
     * Fallback method for circuit breaker - provides alternative path when Kafka is unavailable
     * Logs failure details without blocking the caller
     */
    public CompletionStage<Void> fallbackProduceAccountingEvent(AccountingRequestDto request) {
        fallbackCounter.increment();
        long failures = consecutiveFailures.get();
        LoggingUtil.logError(logger, CLASS_NAME, "fallbackProduceAccountingEvent", null,
                "Circuit breaker activated - Fallback for %s event - SessionId: %s, NasIP: %s (consecutive failures: %d)",
                request.actionType(), request.sessionId(), failures);

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Re-populate MDC on the Kafka producer's network thread so the structured
     * log format ([traceId][userName][sessionId]) renders the request context
     * for ACK/NACK callbacks. MDC is ThreadLocal and does not propagate from
     * the handler thread that called send().
     */
    private static void applyMdc(String traceId, String userName, String sessionId) {
        if (traceId != null) MDC.put(AuthServiceConstants.TRACE_ID, traceId);
        if (userName != null) MDC.put(AuthServiceConstants.PARAM_USER_NAME, userName);
        if (sessionId != null) MDC.put(AuthServiceConstants.SESSION_ID, sessionId);
    }

    /**
     * Optimized partition key builder - uses ThreadLocal StringBuilder to avoid allocations.
     * Calls remove() after each use to prevent memory leaks in thread-pool environments.
     */
    private String buildPartitionKey(String sessionId, String nasIp) {
        StringBuilder sb = PARTITION_KEY_BUILDER.get();
        sb.setLength(0);
        sb.append(sessionId != null ? sessionId : "unknown")
                .append('-')
                .append(nasIp != null ? nasIp : "unknown");
        String result = sb.toString();
        PARTITION_KEY_BUILDER.remove();
        return result;
    }

}