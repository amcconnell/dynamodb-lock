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

    private static final int ACQUISITION_RESOLUTION = 100;
    private static final int DEFAULT_ACQUISITION_TIMEOUT = 10000;
    private static final int DEFAULT_LOCK_EXPIRATION = 60000;

    private static final String LOCK_TABLE = "DYNAMODB_LOCKS";

    private final AmazonDynamoDB client;
    private final String lockKey;
    private final int acquisitionTimeout;
    private final int lockExpiration;
    private final UUID lockUUID;

    public DynamoDBLock(AmazonDynamoDB client, String lockKey, int acquisitionTimeout, int lockExpiration) {
        this.client = client;
        this.lockKey = lockKey;
        this.acquisitionTimeout = acquisitionTimeout;
        this.lockExpiration = lockExpiration;
        lockUUID = UUID.randomUUID();
    }

    public DynamoDBLock(AmazonDynamoDB client, String lockKey) {
        this(client, lockKey, DEFAULT_ACQUISITION_TIMEOUT, DEFAULT_LOCK_EXPIRATION);
    }

    public DynamoDBLock(AmazonDynamoDB client, String lockKey, int acquisitionTimeout) {
        this(client, lockKey, acquisitionTimeout, DEFAULT_LOCK_EXPIRATION);
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
     * Acquire the lock. You can also use this to extend the expiration of the lock.
     * 
     * @return true if lock is acquired, false otherwise
     * @throws InterruptedException
     */
    public synchronized boolean acquire() throws InterruptedException {
        return acquire(client);
    }

    /**
     * Renew the lock. An expired lock may not be renewed.
     * 
     * @return true if lock is acquired, false otherwise
     * @throws InterruptedException
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

                PutItemRequest putItemRequest = new PutItemRequest().withTableName(LOCK_TABLE)
                        .withItem(new ImmutableMap.Builder<String, AttributeValue>()
                                .put("lock_path", new AttributeValue().withS(lockKey))
                                .put("lock_expiration", new AttributeValue().withN(Long.toString(expirationTime)))
                                .put("lock_ttl", new AttributeValue().withN(Long.toString((expirationTime/1000)+1)))
                                .put("lock_owner", new AttributeValue().withS(lockUUID.toString())).build())
                        .withConditionExpression(
                                "attribute_not_exists(lock_path) OR (lock_owner <> :owner AND lock_expiration <= :now) OR (lock_owner = :owner AND lock_expiration > :now)")
                        .withExpressionAttributeValues(expressionAttributeValues);

                client.putItem(putItemRequest);
                return true;
            } catch (ConditionalCheckFailedException e) {
            }

            timeout -= ACQUISITION_RESOLUTION;
            Thread.sleep(ACQUISITION_RESOLUTION);

        }

        return false;

    }

    protected synchronized boolean renew(AmazonDynamoDB client) throws InterruptedException {

        final long expirationTime = System.currentTimeMillis() + lockExpiration;

        //
        // Put a record if:
        // 1. There is an existing lock for this key AND
        // 2. We are the lock owner and the lock hasn't expired
        //
        // Developer Note: It is reasonable to ask if we should allow an *expired* lock to be renewed as long
        // as the previous owner is the one requesting the renewal. For now, that is disallowed.
        //
        try {

            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":owner", new AttributeValue().withS(lockUUID.toString()));
            expressionAttributeValues.put(":now",
                    new AttributeValue().withN(String.valueOf(System.currentTimeMillis())));
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(LOCK_TABLE)
                    .withItem(new ImmutableMap.Builder<String, AttributeValue>()
                            .put("lock_path", new AttributeValue().withS(lockKey))
                            .put("lock_expiration", new AttributeValue().withN(Long.toString(expirationTime)))
                            .put("lock_ttl", new AttributeValue().withN(Long.toString((expirationTime/1000)+1)))
                            .put("lock_owner", new AttributeValue().withS(lockUUID.toString())).build())
                    .withConditionExpression("attribute_exists(lock_path) AND lock_owner = :owner AND lock_expiration > :now")
                    .withExpressionAttributeValues(expressionAttributeValues);

            client.putItem(putItemRequest);
            return true;
        } catch (ConditionalCheckFailedException e) {
        }

        return false;

    }

    protected synchronized void release(AmazonDynamoDB client) {
        try {
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":owner", new AttributeValue().withS(lockUUID.toString()));

            Map<String, AttributeValue> keys = new HashMap<>();
            keys.put("lock_path", new AttributeValue(lockKey));
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(LOCK_TABLE).withKey(keys)
                    .withConditionExpression("lock_owner = :owner")
                    .withExpressionAttributeValues(expressionAttributeValues);
            client.deleteItem(deleteItemRequest);
        } catch (ConditionalCheckFailedException e) {
        }
    }

}
