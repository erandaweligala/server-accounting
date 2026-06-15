package com.csg.airtel.aaa4j.monitoring;

/**
 * High-level categorization of errors observable in the RADIUS server.
 * Used as a Micrometer tag so dashboards / alerts can group errors by
 * the type of failure rather than the specific exception class.
 */
public enum ErrorCategory {

    HTTP_FAILURE("http_failure"),
    HTTP_TIMEOUT("http_timeout"),

    KAFKA_FAILURE("kafka_failure"),
    KAFKA_TIMEOUT("kafka_timeout"),
    KAFKA_CIRCUIT_OPEN("kafka_circuit_open"),

    UDP_FAILURE("udp_failure"),
    UDP_TIMEOUT("udp_timeout"),

    RADIUS_CLIENT_FAILURE("radius_client_failure"),

    PROGRAMMING_EXCEPTION("programming_exception"),
    UNCAUGHT_EXCEPTION("uncaught_exception"),
    REST_UNHANDLED_EXCEPTION("rest_unhandled_exception");

    private final String tagValue;

    ErrorCategory(String tagValue) {
        this.tagValue = tagValue;
    }

    public String tagValue() {
        return tagValue;
    }
}
