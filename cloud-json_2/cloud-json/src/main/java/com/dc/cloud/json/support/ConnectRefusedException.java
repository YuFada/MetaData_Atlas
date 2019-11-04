package com.dc.cloud.json.support;

public class ConnectRefusedException extends RuntimeException {

    public ConnectRefusedException(String message) {
        super(message);
    }

    public ConnectRefusedException(String message, Throwable cause) {
        super(message, cause);
    }
}
