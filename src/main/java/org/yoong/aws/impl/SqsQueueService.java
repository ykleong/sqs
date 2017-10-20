package org.yoong.aws.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.yoong.aws.Message;
import org.yoong.aws.QueueService;
import org.yoong.aws.Message.MessageBuilder;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

public class SqsQueueService implements QueueService {
    
    //
    // Task 4: Optionally implement parts of me.
    //
    // This file is a placeholder for an AWS-backed implementation of QueueService. It is included
    // primarily so you can quickly assess your choices for method signatures in QueueService in
    // terms of how well they map to the implementation intended for a production environment.
    //

    private final AmazonSQS sqsClient;
    
    /*
     * Assuming that the sqsClient has already been initialised when passed into this implementation. 
     */
    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * Create an SQS queue with the interface - setting he visibility timeout on creation.
     */
    @Override
    public String createQueue(String queueName, long visibilityTimeout, TimeUnit unit) {
        
        // Setting visibility Timeout when creating the queue.
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(QueueAttributeName.VisibilityTimeout.toString(), String.valueOf(unit.toSeconds(visibilityTimeout)));
        
        CreateQueueRequest request = new CreateQueueRequest(queueName).withAttributes(attributes);
        
        CreateQueueResult result = sqsClient.createQueue(request);
        
        return result.getQueueUrl();
    }

    @Override
    public void push(String queueUrl, String message) {
        
        sqsClient.sendMessage(queueUrl, message);
    }

    /**
     * SQS's receiveMessage api can retrieve up to 10 messages, but retrieves a single message by default.
     * Return the first message from returned list, or null if empty.
     */
    @Override
    public Message pull(String queueUrl) {
        
        ReceiveMessageResult result = sqsClient.receiveMessage(queueUrl);
        
        List <com.amazonaws.services.sqs.model.Message> msgs = result.getMessages();
        
        // map amazon's sqs Message to this implementations Message and return.
        if(!msgs.isEmpty()) {
            com.amazonaws.services.sqs.model.Message msg = msgs.get(0);
            return new MessageBuilder().setBody(msg.getBody()).setReceiptHandle(msg.getReceiptHandle()).build();
        }
        
        return null;
    }

    /**
     * Delete based on Message returned by pull.
     */
    @Override
    public boolean delete(String queueUrl, Message handle) {

        sqsClient.deleteMessage(queueUrl, handle.getReceiptHandle());
        
        return true;
    }
 
}
