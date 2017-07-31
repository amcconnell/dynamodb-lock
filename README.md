# dynamodb-lock
A distributed locking implementation using DynamoDB written in Java.

### Description

dynamodb-lock is a distributed locking implementation built upon AWS DynamoDB.

### Usage

First, you will need to create a single table in DynamoDB (the default table name is DYNAMODB_LOCKS). It should have a partition key named "lock_path". You may also want to specify a TTL field named "lock_ttl" set to a period that exceeds the longest period of time that you expect an application to hold a lock.

[TBD - example code]

### Requirements

* Java 8
* Amazon Web Services account with DynamoDB access

