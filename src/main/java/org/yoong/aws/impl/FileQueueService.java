package org.yoong.aws.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.server.UID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.yoong.aws.Message;
import org.yoong.aws.QueueService;
import org.yoong.aws.Message.MessageBuilder;
import org.yoong.aws.exceptions.FileQueueException;
import org.yoong.aws.exceptions.NoSuchQueueException;
import org.yoong.aws.util.Clock;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

/**
 * The FileQueueService follows the same idea of the InMemoryQueueService, but replacing the primary and secondary with a file-based implementation. 
 * <li>The FileQueueService requires a home directory </li>
 * <li>A seperate folder is created for each queue within the home directory </li>
 * <li>A FileQueue contains 2 files, a primary and secondary </li> 
 * <li>The FileQueueService can be used across JVMs, within the same Host </li>
 *  
 * <p>
 * <h4>Storing Messages to File</h4>
 * The primary and secondary files store messages per line in a simple csv format, with the following attributes : <br/>
 * <li>receipHandle - unique id for a pulled message (required for deletion) </li>
 * <li>invisibilitytimeoutTime - unix timestamp when invisibility expires </li>
 * <li>body - message body string </li>
 * </p>
 * 
 * <p>
 * <h4>Receipt Handle</h4>
 * When messages are pulled and added to the secondary.  A receiptHandle is generated and set into the message, which is unique across all messages. <br/>
 * This is necessary for the FileQueue implementation, in order to identify the correct record within the file to delete. <br/><br/> 
 * 
 * The id is generated using {@link java.rmi.server.UID} which is suitable for our requirements - being unique for the host they are generated. 
 * </p>
 * 
 * <p>
 * <h4>Synchronization</h4>
 * For simplicity, each queue contains a single lock which is shared by the primary and secondary.
 * </p>
 * 
 * @see InMemoryQueue
 */
public class FileQueueService implements QueueService {

    private final File homeDirectory;

    // cache created file queues 
    private ConcurrentHashMap<String, FileQueue> queues = new ConcurrentHashMap<String, FileQueue>();
    private final Clock clock;
    
    /**
     * Class to hold required objects within a file queue.
     */
    private class FileQueue {

        final String name;
        final File dir;
        final File lock;
        final File primary;
        final File secondary;
        final long visibilityTimeout;

        public FileQueue(File dir, long visibilityTimeout) {
            this.name = dir.getName();
            this.dir = dir;
            this.dir.mkdirs();

            lock = new File(dir, ".lock");
            primary = new File(dir, "primary");
            secondary = new File(dir, "secondary");

            this.visibilityTimeout = visibilityTimeout;
        }

        // implement a lock using file mkdir. 
        void lock() throws InterruptedException {
            while (!lock.mkdir()) {
                Thread.sleep(20);
            }
        }

        void unlock() {
            lock.delete();
        }
    }

    public FileQueueService(File homeDirectory, Clock clock) {
        this.homeDirectory = homeDirectory;
        this.clock = clock;
    }
    
    private String getHashedString(String value) {
        return Hashing.md5().hashString(value, Charsets.UTF_8).toString();
    }

    @Override
    public String createQueue(String queueName, long visibilityTimeout, TimeUnit unit) {

        String hashedName = getHashedString(queueName);
        
        FileQueue queue = queues.get(hashedName);

        if (queue == null) {
            queue = new FileQueue(new File(homeDirectory, hashedName), unit.toMillis(visibilityTimeout));
            queues.putIfAbsent(queue.name, queue);
        }
        
        return queueName;
    }

    @Override
    public void push(String queueName, String message) {

        FileQueue queue = getQueue(queueName);

        try {
            queue.lock();
        } catch (InterruptedException e) {
            throw new FileQueueException("Failed to obtain lock for FileQueue - " + queue.name, e);
        }

        try (PrintWriter pw = new PrintWriter(new FileWriter(queue.primary, true))) {
            // append message to primary
            pw.println(toRecord(new MessageBuilder().setBody(message).build()));
        
        } catch (IOException e) {
            throw new FileQueueException("Caught IO exception in FileQueue - " + queue.name, e);
        } finally {
            queue.unlock();
        }
    }

    @Override
    public Message pull(String queueName) {

        FileQueue queue = getQueue(queueName);
        File buffer = new File(queue.dir, "buffer");

        try {
            queue.lock();
        } catch (InterruptedException e) {
            throw new FileQueueException("Failed to obtain lock for FileQueue - " + queue.name, e);
        }

        // ensure buffer is empty
        buffer.delete();

        try (BufferedReader pr = queue.primary.exists() ? new BufferedReader(new FileReader(queue.primary)) : null;
                BufferedReader sr = queue.secondary.exists() ? new BufferedReader(new FileReader(queue.secondary)) : null;
                PrintWriter sw = new PrintWriter(new FileWriter(queue.secondary, true));
                PrintWriter bw = new PrintWriter(new FileWriter(buffer, true));) {

            // read first line from secondary and primary
            String secondary = (sr != null) ? sr.readLine() : null;
            String primary = (pr != null) ? pr.readLine() : null;

            // get message from head of secondary
            Message msg = (secondary != null) ? fromRecord(secondary) : null;

            // check if head of secondary has surpassed the timeout
            if (msg != null && !msg.isInvisible(clock)) {
            
                // this message will be pulled, reset the visibility timeout
                msg.startInvisible(queue.visibilityTimeout, clock);
                
                // move the record to the end of secondary, with the help of the buffer
                while ((secondary = sr.readLine()) != null) {
                    bw.println(secondary);
                }
                bw.println(toRecord(msg));
                
                queue.secondary.delete();
                buffer.renameTo(queue.secondary);
                
                return msg;
            }

            // otherwise, get head of primary
            msg = (primary != null) ? fromRecord(primary) : null;

            if (msg != null) {
                
                // generate and set the receipt handle, and start the visibility timeout
                msg.setReceiptHandle(new UID().toString());
                msg.startInvisible(queue.visibilityTimeout, clock);
                
                // remove the record from primary and append it to secondary
                while ((primary = pr.readLine()) != null) {
                    bw.println(primary);
                }

                queue.primary.delete();
                buffer.renameTo(queue.primary);
                
                sw.println(toRecord(msg));
                
                return msg;
            }
            
        } catch (IOException e) {
            throw new FileQueueException("Caught IO exception in FileQueue - " + queue.name, e);
        } finally {
            queue.unlock();
        }

        return null;
    }

    @Override
    public boolean delete(String queueName, Message message) {

        boolean deleted = false;

        FileQueue queue = getQueue(queueName);

        if (!queue.secondary.exists()) {
            return false;
        }

        File buffer = new File(queue.dir, "buffer");

        try {
            queue.lock();
        } catch (InterruptedException e) {
            throw new FileQueueException("Failed to obtain lock for FileQueue - " + queue.name, e);
        }

        // ensure buffer is empty
        buffer.delete();

        try (BufferedReader sr = new BufferedReader(new FileReader(queue.secondary));
                PrintWriter bw = new PrintWriter(new FileWriter(buffer, true));) {

            String line;

            // copy records from invisible messages to buffer until we find the record to delete.
            while ((line = sr.readLine()) != null) {
                Message record = fromRecord(line);
                if (record.getReceiptHandle().contentEquals(message.getReceiptHandle())) {
                    deleted = true;
                    break;
                } else {
                    bw.println(line);
                }
            }

            // continue copying the rest of the lines (except the line to delete) to the buffer.
            while ((line = sr.readLine()) != null) {
                bw.println(line);
            }

            // replace secondary with buffer
            queue.secondary.delete();
            buffer.renameTo(queue.secondary);

        } catch (IOException e) {
            throw new FileQueueException("Caught IO exception in FileQueue - " + queue.name, e);
        } finally {
            queue.unlock();
        }

        return deleted;
    }

    private FileQueue getQueue(String queueName) {
        FileQueue queue = queues.get(getHashedString(queueName));
        if (queue == null) {
            throw new NoSuchQueueException("Queue named \"" + queueName + "\" does not exist.");
        }
        return queue;
    }

    /**
     * Method to convert a message object into a record line in a file
     */
    private String toRecord(Message msg) {
        StringBuilder sb = new StringBuilder();
        return sb.append(msg.getReceiptHandle() == null ? "" : msg.getReceiptHandle()).append(",")
                .append(msg.getInvisibleTimeoutTime()).append(",")
                .append(BaseEncoding.base64().encode(msg.getBody().getBytes())).toString();
    }

    /**
     * Simple method to convert a record line from a file into a Message
     */
    private Message fromRecord(String line) {
        String cols[] = line.split(",", 3);
        String receiptHandle = cols[0].isEmpty() ? null : cols[0];
        long invisibleTimeoutTime = Long.valueOf(cols[1]);
        String body = cols[2];
        return new MessageBuilder().setReceiptHandle(receiptHandle)
                .setInvisibleTimeoutTime(invisibleTimeoutTime)
                .setBody(new String(BaseEncoding.base64().decode(body))).build();
    }

}
