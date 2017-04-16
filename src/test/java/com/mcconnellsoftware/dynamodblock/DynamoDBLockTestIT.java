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

        DynamoDBLock lock = DynamoDBLock.amazonDynamoDB(client).lockKey("testLock").build();
        assertTrue(lock.acquire());

        DynamoDBLock lock2 = DynamoDBLock.amazonDynamoDB(client).lockKey("testLock").acquisitionTimeout(1000).build();
        assertFalse(lock2.acquire());

        lock.release();

        lock2 = DynamoDBLock.amazonDynamoDB(client).lockKey("testLock").acquisitionTimeout(1000).build();
        assertTrue(lock2.acquire());
        lock2.release();
    }

    @Test
    public void testRenew() throws InterruptedException {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBLock lock = DynamoDBLock.amazonDynamoDB(client).lockKey("testrenew").build();
        assertTrue(lock.acquire());

        Thread.sleep(2000l);

        assertTrue(lock.renew());

        lock.release();

        DynamoDBLock lock2 = DynamoDBLock.amazonDynamoDB(client).lockKey("testrenew").acquisitionTimeout(1000).build();
        assertTrue(lock2.acquire());
        lock2.release();
    }

    @Test
    public void testRenewAferExpiration() throws InterruptedException {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBLock lock = DynamoDBLock.amazonDynamoDB(client).lockKey("testrenewexp").acquisitionTimeout(15000)
                .lockExpiration(2000).build();
        assertTrue(lock.acquire());

        Thread.sleep(3000l);

        assertFalse(lock.renew());

        lock.release();

    }

    @Test
    public void testReleaseWithoutLock() throws InterruptedException {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

        DynamoDBLock lock = DynamoDBLock.amazonDynamoDB(client).lockKey("releasenolock").acquisitionTimeout(15000)
                .lockExpiration(2000).build();
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
                    DynamoDBLock lock = DynamoDBLock.amazonDynamoDB(client).lockKey("testlock")
                            .acquisitionTimeout(15000).lockExpiration(200).build();
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
