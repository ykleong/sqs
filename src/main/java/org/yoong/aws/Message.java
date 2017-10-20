package org.yoong.aws;

import org.yoong.aws.util.Clock;

public class Message {

    private String body;
    // only set when pulled from a queue.
    private String receiptHandle;
    private long invisibleTimeoutTime;
    
    private Message(String receiptHandle, long invisibleTimeoutTime, String body) {
        this.receiptHandle = receiptHandle;
        this.invisibleTimeoutTime = invisibleTimeoutTime;
        this.body = body;
    }

    protected void setBody(String body) {
        this.body = body;
    }
    
    public String getBody() {
        return body;
    }
    
    public String getReceiptHandle() {
        return receiptHandle;
    }

    public long getInvisibleTimeoutTime() {
        return invisibleTimeoutTime;
    }
    
    /**
     * Check whether a message is invisible by comparing current system time with the timeout time.
     * @return
     */
    public boolean isInvisible(Clock clock) {
        if (clock.getCurrentTime() >= invisibleTimeoutTime) {
            return false;
        }
        return true;
    }
    
    /**
     * Start invisible timeout by setting the future unix timestamp when the timeout should expire.
     * @param invisibleTimeout in milliseconds
     */
    public void startInvisible(long invisibleTimeout, Clock clock) {
        this.invisibleTimeoutTime = clock.getCurrentTime() + invisibleTimeout;
    }
    
    public void setReceiptHandle(String receiptHandle) {
        this.receiptHandle = receiptHandle;
    }
    
    /**
     * Builder for Message class.
     */
    public static class MessageBuilder {
        
        private String receiptHandle;
        private String body;
        private long invisibleTimeoutTime = 0;
        
        public MessageBuilder() {
            
        }
        
        public MessageBuilder setBody(String body) {
            this.body = body;
            return this;
        }
        
        public MessageBuilder setReceiptHandle(String receiptHandle) {
            this.receiptHandle = receiptHandle;
            return this;
        }
        
        public MessageBuilder setInvisibleTimeoutTime(long invisibleTimeoutTime) {
            this.invisibleTimeoutTime = invisibleTimeoutTime;
            return this;
        }
        
        public Message build() {
            return new Message(receiptHandle, invisibleTimeoutTime, body);
        }
    }
}
