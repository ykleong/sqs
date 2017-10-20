package org.yoong.aws.impl;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.yoong.aws.Message;
import org.yoong.aws.QueueService;
import org.yoong.aws.Message.MessageBuilder;
import org.yoong.aws.exceptions.NoSuchQueueException;
import org.yoong.aws.util.Clock;

/**
 * Implementation of a single in-memory queue involves two queues (primary and secondary) to support reliability requirements. 
 * 
 * <p>
 * <h4>Push</h4>
 * Pushing adds messages only to the primary.
 * </p>
 * 
 * <p>
 * <h4>Pull</h4>
 * When a message is pulled successfully, it's visibility timeout is set and it is moved to the tail of the secondary queue. <br/>
 * The message is pulled from the head of secondary, if it is not empty and the message has surpassed it's visibility timeout. <br/>  
 * Otherwise, the message is pulled from the head of the primary.
 * </p>
 * 
 * <p>
 * Assuming that the visibility timeout is constant, the contents of the secondary queue will always be sorted by the visibility timeout. <br/>
 * Thus, the secondary will always contain a list of pulled messages with the first to timeout at the head. <br/>
 * If a variable timeout is required, it is only a matter of ensuring the secondary is sorted when messages are added to it.
 * </p>
 * <p>
 * Checking for visibility timeouts is implemented with simple timestamps. see {@link Message#isInvisible}
 * </p>
 * 
 * <p>
 * <h4>Delete</h4>
 * Deleting a message only deletes the specified message from the secondary. <br/>
 * This also helps to enforce the reliability requirement that only messages that have been pulled can get deleted.
 * </p>
 * 
 * <p>
 * <h4>Synchronization</h4>
 * The secondary requires synchronization blocks in the pull() and delete() method. <br/>  
 * This is to prevent the following situations : </br>
 * <li>Multiple processes peeking at and being delivered the same message</li>
 * <li>A message that has been deleted, but gets added back to the queue during a pull</li>
 * 
 */
public class InMemoryQueueService implements QueueService {

    /**
     * Concurrent hash map to store queues with queue name as key.
     */
    private ConcurrentHashMap<String, InMemoryQueue> queues = new ConcurrentHashMap<String, InMemoryQueue>();
    private final Clock clock;
    
    /**
     * Class to hold required objects within an in memory queue.
     */
    private class InMemoryQueue {

        final String name;
        final Queue<Message> primary = new ConcurrentLinkedQueue<Message>();
        final Queue<Message> secondary = new ConcurrentLinkedQueue<Message>();
        final long visibilityTimeout;
        final Clock clock;

        public InMemoryQueue(String name, long visibilityTimeout, Clock clock) {
            this.name = name;
            this.visibilityTimeout = visibilityTimeout;
            this.clock = clock;
        }
    }

    public InMemoryQueueService(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String createQueue(String queueName, long visibilityTimeout, TimeUnit unit) {

        InMemoryQueue queue = queues.get(queueName);

        if (queue == null) {
            queue = new InMemoryQueue(queueName, unit.toMillis(visibilityTimeout), clock);
            queues.putIfAbsent(queue.name, queue);
        }
        
        return queue.name;
    }

    @Override
    public void push(String queueName, String message) {
        // push message to tail of primary queue
        getQueue(queueName).primary.add(new MessageBuilder().setBody(message).build());
    }

    @Override
    public Message pull(String queueName) {

        InMemoryQueue queue = getQueue(queueName);

        synchronized (queue.secondary) {

            Message secMsg = queue.secondary.peek();

            // peek secondary queue to check if invisibility has timed out
            if (secMsg != null && !secMsg.isInvisible(clock)) {
                // set the visibility timeout and move msg to the tail of the secondary queue
                secMsg.startInvisible(queue.visibilityTimeout, clock);
                queue.secondary.remove(secMsg);
                queue.secondary.add(secMsg);
                return secMsg;
            }
        }

        Message priMsg = queue.primary.poll();

        if (priMsg != null) {
            // set the visibility timeout and move msg from primary to secondary queue.
            priMsg.startInvisible(queue.visibilityTimeout, clock);
            queue.secondary.add(priMsg);
        }

        return priMsg;
    }

    @Override
    public boolean delete(String queueName, Message message) {

        InMemoryQueue queue = getQueue(queueName);

        synchronized (queue.secondary) {
            return queue.secondary.remove(message);
        }
    }

    private InMemoryQueue getQueue(String queueName) {

        InMemoryQueue queue = queues.get(queueName);
        if (queue == null) {
            throw new NoSuchQueueException("Queue named \"" + queueName + "\" does not exist.");
        }
        return queue;
    }
}
