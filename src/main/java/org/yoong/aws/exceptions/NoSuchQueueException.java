package org.yoong.aws.exceptions;

public class NoSuchQueueException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;

    public NoSuchQueueException(String msg) {
        super(msg);
    }
}
