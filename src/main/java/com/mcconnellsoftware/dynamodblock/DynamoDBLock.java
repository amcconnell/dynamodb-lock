package com.mcconnellsoftware.dynamodblock;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.google.common.collect.ImmutableMap;

public class DynamoDBLock {

    // Default values
    private static final String DEFAULT_LOCK_TABLE = "DYNAMODB_LOCKS";
    private static final int DEFAULT_ACQUISITION_TIMEOUT = 10000;
    private static final int DEFAULT_LOCK_EXPIRATION = 60000;
    private static final int DEFAULT_ACQUISITION_RESOLUTION = 100;

    // Required fields
    private AmazonDynamoDB client;
    private String lockKey;

    // Optional fields
    private String lockTable = DEFAULT_LOCK_TABLE;
    private int acquisitionTimeout = DEFAULT_ACQUISITION_TIMEOUT;
    private int lockExpiration = DEFAULT_LOCK_EXPIRATION;
    private int acquisitionResolution = DEFAULT_ACQUISITION_RESOLUTION;

    // Internal fields
    private UUID lockUUID;

    private DynamoDBLock() {
    }

    public static ILockKey amazonDynamoDB(AmazonDynamoDB client) {
        return new DynamoDBLock.Builder(client);
    }

    public interface ILockKey {
        IBuild lockKey(String lockKey);
    }

    public interface IBuild {

        IBuild acquisitionTimeout(int acquisitionTimeout);

        IBuild acquisitionResolution(int acquisitionResolution);

        IBuild lockExpiration(int lockExpiration);

        IBuild lockTable(String lockTable);

        DynamoDBLock build();
    }

    /*
     * Builder pattern is based on this blog post:
     * http://blog.crisp.se/2013/10/09/perlundholm/another-builder-pattern-for-
     * java
     */
    private static class Builder implements ILockKey, IBuild {

        private final DynamoDBLock instance = new DynamoDBLock();

        public Builder(AmazonDynamoDB client) {
            instance.client = client;
        }

        @Override
        public IBuild lockKey(String lockKey) {
            instance.lockKey = lockKey;
            return this;
        }

        @Override
        public DynamoDBLock build() {
            instance.lockUUID = UUID.randomUUID();
            return instance;
        }

        @Override
        public IBuild acquisitionTimeout(int acquisitionTimeout) {
            instance.acquisitionTimeout = acquisitionTimeout;
            return this;
        }

        @Override
        public IBuild acquisitionResolution(int acquisitionResolution) {
            instance.acquisitionResolution = acquisitionResolution;
            return this;
        }

        @Override
        public IBuild lockExpiration(int lockExpiration) {
            instance.lockExpiration = lockExpiration;
            return this;
        }

        @Override
        public IBuild lockTable(String lockTable) {
            instance.lockTable = lockTable;
            return this;
        }
    }

    /**
     * @return UUID identifying this lock's owner
     */
    public UUID getLockOwner() {
        return lockUUID;
    }

    /**
     * @return The key globally identifying this lock
     */
    public String getLockKey() {
        return lockKey;
    }

    /**
     * Acquire the lock. You can also use this to extend the expiration of the
     * lock.
     * 
     * @return true if lock is acquired, false otherwise
     * @throws InterruptedException if the operation is interrupted
     */
    public synchronized boolean acquire() throws InterruptedException {
        return acquire(client);
    }

    /**
     * Renew the lock. An expired lock may not be renewed.
     * 
     * @return true if lock is acquired, false otherwise
     * @throws InterruptedException if the operation is interrupted
     */
    public synchronized boolean renew() throws InterruptedException {
        return renew(client);
    }

    /**
     * Release the lock
     */
    public synchronized void release() {
        release(client);
    }

    protected synchronized boolean acquire(AmazonDynamoDB client) throws InterruptedException {

        boolean acquired = false;

        int timeout = acquisitionTimeout;

        while (timeout >= 0) {

            final long expirationTime = System.currentTimeMillis() + lockExpiration;

            //
            // Put a record if:
            // 1. There is no existing lock for this key OR
            // 2. We are not the lock owner, and the lock has expired OR
            // 3. We are the lock owner, and the lock hasn't expired
            //
            try {

                Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                expressionAttributeValues.put(":owner", new AttributeValue().withS(lockUUID.toString()));
                expressionAttributeValues.put(":now",
                        new AttributeValue().withN(String.valueOf(System.currentTimeMillis())));

                PutItemRequest putItemRequest = new PutItemRequest().withTableName(lockTable)
                        .withItem(new ImmutableMap.Builder<String, AttributeValue>()
                                .put("lock_path", new AttributeValue().withS(lockKey))
                                .put("lock_expiration", new AttributeValue().withN(Long.toString(expirationTime)))
                                .put("lock_ttl", new AttributeValue().withN(Long.toString((expirationTime / 1000) + 1)))
                                .put("lock_owner", new AttributeValue().withS(lockUUID.toString())).build())
                        .withConditionExpression(
                                "attribute_not_exists(lock_path) OR (lock_owner <> :owner AND lock_expiration <= :now) OR (lock_owner = :owner AND lock_expiration > :now)")
                        .withExpressionAttributeValues(expressionAttributeValues);

                client.putItem(putItemRequest);
                acquired = true;
                break;
            } catch (ConditionalCheckFailedException e) {
            }

            timeout -= acquisitionResolution;
            Thread.sleep(acquisitionResolution);

        }

        return acquired;

    }

    protected synchronized boolean renew(AmazonDynamoDB client) throws InterruptedException {

        boolean renewed = false;

        final long expirationTime = System.currentTimeMillis() + lockExpiration;

        //
        // Put a record if:
        // 1. There is an existing lock for this key AND
        // 2. We are the lock owner and the lock hasn't expired
        //
        // Developer Note: It is reasonable to ask if we should allow an
        // *expired* lock to be renewed as long
        // as the previous owner is the one requesting the renewal. For now,
        // that is disallowed.
        //
        try {

            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":owner", new AttributeValue().withS(lockUUID.toString()));
            expressionAttributeValues.put(":now",
                    new AttributeValue().withN(String.valueOf(System.currentTimeMillis())));
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(lockTable)
                    .withItem(new ImmutableMap.Builder<String, AttributeValue>()
                            .put("lock_path", new AttributeValue().withS(lockKey))
                            .put("lock_expiration", new AttributeValue().withN(Long.toString(expirationTime)))
                            .put("lock_ttl", new AttributeValue().withN(Long.toString((expirationTime / 1000) + 1)))
                            .put("lock_owner", new AttributeValue().withS(lockUUID.toString())).build())
                    .withConditionExpression(
                            "attribute_exists(lock_path) AND lock_owner = :owner AND lock_expiration > :now")
                    .withExpressionAttributeValues(expressionAttributeValues);

            client.putItem(putItemRequest);
            renewed = true;
        } catch (ConditionalCheckFailedException e) {
        }

        return renewed;

    }

    protected synchronized void release(AmazonDynamoDB client) {
        try {
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":owner", new AttributeValue().withS(lockUUID.toString()));

            Map<String, AttributeValue> keys = new HashMap<>();
            keys.put("lock_path", new AttributeValue(lockKey));
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(lockTable).withKey(keys)
                    .withConditionExpression("lock_owner = :owner")
                    .withExpressionAttributeValues(expressionAttributeValues);
            client.deleteItem(deleteItemRequest);
        } catch (ConditionalCheckFailedException e) {
        }
    }

}
