package org.yoong.aws;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.yoong.aws.Message;
import org.yoong.aws.Message.MessageBuilder;
import org.yoong.aws.exceptions.NoSuchQueueException;
import org.yoong.aws.impl.FileQueueService;
import org.yoong.aws.util.Clock;

public class FileQueueTest {

    private final long timeout = 50000;
    private final String queueName = "queue/../foo";
    private final String messageBody = "message\nmessage";

    private final File serviceDirectory = new File("FileQueueTest");

    /**
     * Ensure the service's home directory is empty before each test.
     */
    @Before
    public void before() throws IOException {
        cleanUp(serviceDirectory);
    }

    /**
     * Test basic flows: push - pull - delete. And test for FIFO order.
     */
    @Test
    public void testBasicFlow() {

        FileQueueService service = new FileQueueService(serviceDirectory, new Clock());
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
     * Test that queueservice works across JVMs. By initializing two new services with the same directory and queue
     * name.
     */
    @Test
    public void testCrossJvm() {

        FileQueueService service = new FileQueueService(serviceDirectory, new Clock());
        String url = service.createQueue(queueName, timeout, TimeUnit.MILLISECONDS);

        service.push(url, messageBody);

        FileQueueService service2 = new FileQueueService(serviceDirectory, new Clock());
        String url2 = service2.createQueue(queueName, timeout, TimeUnit.MILLISECONDS);

        Assert.assertEquals(messageBody, service2.pull(url2).getBody());
    }

    /**
     * Test deleting messages from a queue.
     */
    @Test
    public void testQueueDeletion() {

        FileQueueService service = new FileQueueService(serviceDirectory, new Clock());
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
     * Test pulling from a newly created queue. This test ensures there's no bugs within the code around uninitialized
     * objects (eg the primary and secondary)
     */
    @Test
    public void testEmptyQueue() {

        FileQueueService service = new FileQueueService(serviceDirectory, new Clock());
        String url = service.createQueue(queueName, timeout, TimeUnit.MILLISECONDS);

        Assert.assertNull(service.pull(url));
    }

    @Test(expected = NoSuchQueueException.class)
    public void testQueueDoesNotExist() {

        FileQueueService service = new FileQueueService(serviceDirectory, new Clock());
        service.push("test", messageBody);
    }

    /**
     * Test that a message becomes invisible after the first pull, and remains in the queue
     */
    @Test
    public void testVisibility() {

        FileQueueService service = new FileQueueService(serviceDirectory, new Clock());
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
     * Note: On it's own, this test does not assert that the message became invisible and timed out. But together with
     * the above test, it confirms the behaviour.
     */
    @Test
    public void testVisibilityTimeout() {

        FileQueueService service = new FileQueueService(serviceDirectory, new Clock());

        // set visibility timeout to 0
        String url = service.createQueue(queueName, 0, TimeUnit.MILLISECONDS);

        // push a single message to the queue
        service.push(url, messageBody);

        // first and second pulls should return the same message, since it times out immediately
        // - with a longer timeout, the 2nd pull would usually return null (as tested in 'testVisibility')
        Message response = service.pull(url);
        Assert.assertEquals(response.getReceiptHandle(), service.pull(url).getReceiptHandle());
    }

    /**
     * Ensure the service's home directory is cleared after each test.
     */
    @After
    public void after() throws IOException {
        cleanUp(serviceDirectory);
    }

    private void cleanUp(File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

}
