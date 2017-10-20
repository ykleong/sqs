package org.yoong.aws.exceptions;

public class FileQueueException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;

    public FileQueueException(String msg) {
        super(msg);
    }
    
    public FileQueueException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
