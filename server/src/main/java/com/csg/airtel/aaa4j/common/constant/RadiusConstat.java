package com.csg.airtel.aaa4j.common.constant;

public class RadiusConstat {

    private RadiusConstat() {
        // Prevent instantiation
    }

    public static final int COA_DISCONNECT_REQ = 40;
    public static final int COA_ACK = 41;
    public static final int COA_NAK = 42;
    public static final int RADIUS_HEADER_LENGTH = 20;
    public static final int REQUEST_AUTH_OFFSET = 4;
    public static final int REQUEST_AUTH_LENGTH = 16;
    public static final int DISCONNECT_UDP_TIMEOUT_MS = 5000;
    public static final String INITIATE = "initiate";
    public static final String PROCESS_RESPONSE = "processResponse";
    public static final String SEND_COA_REQUEST = "sendCoaRequest";
    public static final String SEND_DISCONNECT_REQUEST = "sendDisconnectRequest";
    public static final String STATUS_ERROR = "error";
    public static final String STATUS_UNKNOWN = "unknown";
}
