package com.csg.airtel.aaa4j.monitoring;

import io.micrometer.core.instrument.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

/**
 * Single source of truth for RADIUS request/response metrics.
 *
 * All meters are pre-registered at construction (no per-call builder allocation).
 * Tagged meters used by the CoA disconnect flow are cached by tag key so that
 * record sites perform a single map lookup and one Timer/Counter call.
 */
@ApplicationScoped
public class RadiusMetricsService {

    private static final String TAG_STATUS = "status";
    private static final String TAG_SERVER = "server";

    private static final String AUTH_TIMER = "radius.authentication.response.time";
    private static final String AUTH_LAST_GAUGE = "radius.authentication.last.response.time.ms";
    private static final String ACCT_TIMER = "radius.accounting.response.time";
    private static final String ACCT_LAST_GAUGE = "radius.accounting.last.response.time.ms";
    private static final String DISCONNECT_TIMER = "radius.coa.disconnect.duration";
    private static final String DISCONNECT_COUNTER = "radius.coa.disconnect.requests";
    private static final String DISCONNECT_LAST_GAUGE = "radius.coa.disconnect.last.response.time.ms";

    // Once a "last response time" gauge hasn't been updated for this long, it
    // reports NaN so dashboards/alerts treat the value as stale instead of
    // drawing a flat line through idle periods.
    private static final long LAST_RESPONSE_STALE_AFTER_NANOS = Duration.ofSeconds(30).toNanos();

    private final MeterRegistry meterRegistry;

    private final Timer authResponseTimer;
    private final Timer accountingResponseTimer;

    private final AtomicLong lastAuthResponseTimeMs = new AtomicLong(0);
    private final AtomicLong lastAuthResponseUpdateNanos = new AtomicLong(0);
    private final AtomicLong lastAccountingResponseTimeMs = new AtomicLong(0);
    private final AtomicLong lastAccountingResponseUpdateNanos = new AtomicLong(0);
    private final AtomicLong lastDisconnectResponseTimeMs = new AtomicLong(0);
    private final AtomicLong lastDisconnectResponseUpdateNanos = new AtomicLong(0);

    private final ConcurrentMap<String, Timer> disconnectTimers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> disconnectCounters = new ConcurrentHashMap<>();

    @Inject
    public RadiusMetricsService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        this.authResponseTimer = Timer.builder(AUTH_TIMER)
                .description("Time to complete a RADIUS authentication request")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .publishPercentileHistogram()
                .serviceLevelObjectives(
                        Duration.ofMillis(10),
                        Duration.ofMillis(20),
                        Duration.ofMillis(50),
                        Duration.ofMillis(100),
                        Duration.ofMillis(250),
                        Duration.ofMillis(500),
                        Duration.ofMillis(1000),
                        Duration.ofMillis(2500),
                        Duration.ofMillis(5000))
                .register(meterRegistry);

        this.accountingResponseTimer = Timer.builder(ACCT_TIMER)
                .description("Time to complete a RADIUS accounting request")
                .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                .publishPercentileHistogram()
                .serviceLevelObjectives(
                        Duration.ofMillis(10),
                        Duration.ofMillis(20),
                        Duration.ofMillis(50),
                        Duration.ofMillis(100),
                        Duration.ofMillis(250),
                        Duration.ofMillis(500),
                        Duration.ofMillis(1000),
                        Duration.ofMillis(2500),
                        Duration.ofMillis(5000))
                .register(meterRegistry);

        Gauge.builder(AUTH_LAST_GAUGE, this, freshLastResponseTime(lastAuthResponseTimeMs, lastAuthResponseUpdateNanos))
                .description("Duration in milliseconds of the most recently completed authentication request (NaN if idle)")
                .register(meterRegistry);

        Gauge.builder(ACCT_LAST_GAUGE, this, freshLastResponseTime(lastAccountingResponseTimeMs, lastAccountingResponseUpdateNanos))
                .description("Duration in milliseconds of the most recently completed accounting request (NaN if idle)")
                .register(meterRegistry);

        Gauge.builder(DISCONNECT_LAST_GAUGE, this, freshLastResponseTime(lastDisconnectResponseTimeMs, lastDisconnectResponseUpdateNanos))
                .description("Duration in milliseconds of the most recently completed CoA Disconnect-Request (NaN if idle)")
                .register(meterRegistry);
    }

    public void recordAuthenticationResponse(Duration duration) {
        authResponseTimer.record(duration);
        lastAuthResponseTimeMs.set(duration.toMillis());
        lastAuthResponseUpdateNanos.set(System.nanoTime());
    }

    public void recordAccountingResponse(Duration duration) {
        accountingResponseTimer.record(duration);
        lastAccountingResponseTimeMs.set(duration.toMillis());
        lastAccountingResponseUpdateNanos.set(System.nanoTime());
    }

    public void recordDisconnectResponse(long elapsedMillis, String server, String status) {
        disconnectTimer(server, status).record(elapsedMillis, TimeUnit.MILLISECONDS);
        lastDisconnectResponseTimeMs.set(elapsedMillis);
        lastDisconnectResponseUpdateNanos.set(System.nanoTime());
    }

    private static ToDoubleFunction<RadiusMetricsService> freshLastResponseTime(AtomicLong valueMs, AtomicLong updatedNanos) {
        return svc -> {
            long updated = updatedNanos.get();
            if (updated == 0L || System.nanoTime() - updated > LAST_RESPONSE_STALE_AFTER_NANOS) {
                return Double.NaN;
            }
            return valueMs.get();
        };
    }

    public void incrementDisconnectRequest(String server, String status) {
        disconnectCounter(server, status).increment();
    }

    private Timer disconnectTimer(String server, String status) {
        return disconnectTimers.computeIfAbsent(tagKey(server, status), key ->
                Timer.builder(DISCONNECT_TIMER)
                        .description("RADIUS CoA Disconnect-Request response time")
                        .tags(Tags.of(TAG_STATUS, status, TAG_SERVER, server))
                        .publishPercentiles(0.5, 0.75, 0.90, 0.95, 0.99)
                        .publishPercentileHistogram()
                        .serviceLevelObjectives(
                                Duration.ofMillis(10),
                                Duration.ofMillis(20),
                                Duration.ofMillis(50),
                                Duration.ofMillis(100),
                                Duration.ofMillis(250),
                                Duration.ofMillis(500),
                                Duration.ofMillis(1000),
                                Duration.ofMillis(2500),
                                Duration.ofMillis(5000))
                        .register(meterRegistry));
    }

    private Counter disconnectCounter(String server, String status) {
        return disconnectCounters.computeIfAbsent(tagKey(server, status), key ->
                Counter.builder(DISCONNECT_COUNTER)
                        .description("Total number of RADIUS CoA Disconnect-Requests sent")
                        .tags(Tags.of(TAG_STATUS, status, TAG_SERVER, server))
                        .register(meterRegistry));
    }

    private static String tagKey(String server, String status) {
        return server + '|' + status;
    }
}
