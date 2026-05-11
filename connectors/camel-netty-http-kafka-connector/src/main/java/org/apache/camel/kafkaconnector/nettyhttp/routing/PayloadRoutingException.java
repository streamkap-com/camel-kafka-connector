package org.apache.camel.kafkaconnector.nettyhttp.routing;

public class PayloadRoutingException extends RuntimeException {

    public PayloadRoutingException(String message) {
        super(message);
    }

    public PayloadRoutingException(String message, Throwable cause) {
        super(message, cause);
    }
}
