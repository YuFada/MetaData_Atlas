package com.dc.cloud.json.support;

public class JsonParseException extends IllegalArgumentException {

    public JsonParseException(Throwable cause) {
        super("Cannot parse JSON", cause);
    }

    public JsonParseException(String message, Throwable cause) {
        super(message, cause);
    }
}
