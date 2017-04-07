package com.mcconnellsoftware.dynamodblock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.mcconnellsoftware.dynamodblock.DynamoDBLock;

public class DynamoDBLockTestIT {

    @BeforeClass
    public static void setup() {
    }

    @Test
    public void testAcquire() throws InterruptedException {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBLock lock = new DynamoDBLock(client, "testlock");
        assertTrue(lock.acquire());

        DynamoDBLock lock2 = new DynamoDBLock(client, "testlock", 1000);
        assertFalse(lock2.acquire());

        lock.release();

        lock2 = new DynamoDBLock(client, "testlock", 1000);
        assertTrue(lock2.acquire());
        lock2.release();
    }

    @Test
    public void testRenew() throws InterruptedException {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBLock lock = new DynamoDBLock(client, "testrenew");
        assertTrue(lock.acquire());

        Thread.sleep(2000l);

        assertTrue(lock.renew());

        lock.release();

        DynamoDBLock lock2 = new DynamoDBLock(client, "testrenew", 1000);
        assertTrue(lock2.acquire());
        lock2.release();
    }

    @Test
    public void testRenewAferExpiration() throws InterruptedException {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBLock lock = new DynamoDBLock(client, "testrenewexp",15000,2000);
        assertTrue(lock.acquire());

        Thread.sleep(3000l);

        assertFalse(lock.renew());

        lock.release();

    }
    
    @Test
    public void testReleaseWithoutLock() throws InterruptedException {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBLock lock = new DynamoDBLock(client, "releasenolock",15000,2000);
        lock.release();

    }
    
    @Test
    public void testConcurrency() throws InterruptedException {
        final int count = 10;

        ConcurrentLocker[] lockers = new ConcurrentLocker[] { new ConcurrentLocker(count), new ConcurrentLocker(count),
                new ConcurrentLocker(count) };

        for (ConcurrentLocker locker : lockers) {
            locker.start();
        }

        for (ConcurrentLocker locker : lockers) {
            locker.join();
        }

        for (ConcurrentLocker locker : lockers) {
            assertEquals(count, locker.count());
        }
    }

    private class ConcurrentLocker extends Thread {

        private final int times;
        private int counter;

        public ConcurrentLocker(int times) {
            this.times = times;
            this.counter = 0;
        }

        public void run() {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
            try {
                for (int i = 0; i < times; i++) {
                    DynamoDBLock lock = new DynamoDBLock(client, "testlock", 15000, 200);
                    try {
                        if (lock.acquire()) {
                            counter++;
                            Thread.sleep(250);
                            lock.release();
                        }
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } finally {
            }
        }

        public int count() {
            return counter;
        }
    };

}
