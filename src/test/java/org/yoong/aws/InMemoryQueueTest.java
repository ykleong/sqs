package org.yoong.aws;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.yoong.aws.Message;
import org.yoong.aws.QueueService;
import org.yoong.aws.Message.MessageBuilder;
import org.yoong.aws.exceptions.NoSuchQueueException;
import org.yoong.aws.impl.InMemoryQueueService;
import org.yoong.aws.util.Clock;

public class InMemoryQueueTest {

    private final long timeout = 30000;
    private final String queueName = "queue";
    private final String messageBody = "message";

    /**
     * Test basic flows: push - pull - delete.
     * And test for FIFO order.
     */
    @Test
    public void testBasicFlow() {

        QueueService service = new InMemoryQueueService(new Clock());
        String url = service.createQueue(queueName, timeout, TimeUnit.MILLISECONDS);

        for (int i = 0; i < 3; i++) {
            service.push(url, messageBody + i);
        }

        for (int i = 0; i < 3; i++) {
            Message response = service.pull(url);
            Assert.assertEquals(messageBody + i, response.getBody());
            Assert.assertTrue(service.delete(url, response));
        }
        
        // test that queue is empty
        Assert.assertNull(service.pull(url));
    }

    /**
     * Test deleting messages from a queue.
     */
    @Test
    public void testQueueDeletion() {

        InMemoryQueueService service = new InMemoryQueueService(new Clock());
        String url = service.createQueue(queueName, timeout, TimeUnit.MILLISECONDS);

        // deleting a random message should fail
        Assert.assertFalse(service.delete(url, new MessageBuilder().build()));
        
        // deleting a pull message should succeed
        service.push(url, messageBody);
        Message msg = service.pull(url);
        Assert.assertTrue(service.delete(url, msg));
        Assert.assertNull(service.pull(url));
    }

    /**
     * Test pulling from a newly created queue.
     * This test ensures there's no bugs within the code around uninitialized objects (eg the primary and secondary)
     */
    @Test
    public void testEmptyQueue() {

        InMemoryQueueService service = new InMemoryQueueService(new Clock());
        String url = service.createQueue(queueName, timeout, TimeUnit.MILLISECONDS);

        Assert.assertNull(service.pull(url));
    }

    
    @Test(expected = NoSuchQueueException.class)
    public void testQueueDoesNotExist() {

        InMemoryQueueService service = new InMemoryQueueService(new Clock());
        service.push("test", messageBody);
    }

    /**
     * Test that a message becomes invisible after the first pull, and remains in the queue
     */
    @Test
    public void testVisibility() {

        InMemoryQueueService service = new InMemoryQueueService(new Clock());
        String url = service.createQueue(queueName, timeout, TimeUnit.MILLISECONDS);

        // push a single message
        service.push(url, messageBody);

        // first pull succeeds
        Message response = service.pull(url);
        Assert.assertEquals(messageBody, response.getBody());
        
        // second pull returns null, because the msg is invisible now.
        Assert.assertNull(service.pull(url));
        
        // delete succeeds, because it is still in the queue, but invisible
        Assert.assertTrue(service.delete(url, response));
    }

    /**
     * Test that a message becomes visible again after a timeout.
     * 
     * Note: On it's own, this test does not assert that the message became invisible and timed out
     * But together with the above test, it confirms the behaviour.
     */
    @Test
    public void testVisibilityTimeout() {

        Clock testClock = Mockito.mock(Clock.class);
        
        Long timeout = 30L;
        
        Mockito.when(testClock.getCurrentTime()).thenReturn(0L, TimeUnit.SECONDS.toMillis(timeout) );
        
        InMemoryQueueService service = new InMemoryQueueService(testClock);

        // set visibility timeout to 0
        String url = service.createQueue(queueName, 30, TimeUnit.SECONDS);

        // push a single message to the queue
        service.push(url, messageBody);

        // first and second pulls should return the same message, since it times out immediately
        // - with a longer timeout, the 2nd pull would usually return null (as tested in 'testVisibility')
        Message response = service.pull(url);
        
        Assert.assertEquals(response, service.pull(url));
    }

}
