package org.yoong.aws;

import java.util.concurrent.TimeUnit;

public interface QueueService {

    /**
     * Method to create a queue based on a unique name.
     * 
     * @param queueName - name of queue
     * @return created queue's URL
     */
    public String createQueue(String queueName, long visibilityTimeout, TimeUnit unit);
    
    /**
     * Method to push a string message to a queue 
     * 
     * @param queueUrl - url of queue obtained from createQueue method.
     * @param message - message string to push to queue
     */
    public void push(String queueUrl, String message);

    /**
     * Method to pull a message from a queue.
     * 
     * @param queueUrl - url of queue obtained from createQueue method.
     * @return Message object pulled from queue, null if queue is empty
     * @see {@link Message}
     */
    public Message pull(String queueUrl);

    /**
     * Method to delete a message from a queue.  
     * This method requires a Message object, that can only be retrieved after a successful {@link QueueService#pull}. 
     *  
     * @param queueUrl - url of queue obtained from createQueue method.
     * @param handle - Message object to delete from queue
     * @return <code>true</code> if succesfully deleted, <code>false</code> otherwise 
     */
    public boolean delete(String queueUrl, Message handle);

}
