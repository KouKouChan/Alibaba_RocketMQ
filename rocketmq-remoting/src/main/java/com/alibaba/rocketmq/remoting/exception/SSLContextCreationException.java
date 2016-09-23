package com.alibaba.rocketmq.remoting.exception;

public class SSLContextCreationException extends RuntimeException {

    public SSLContextCreationException(String message) {
        super(message);
    }

    public SSLContextCreationException(String message, Exception cause) {
        super(message, cause);
    }
}
