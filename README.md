# dynamodb-lock
A distributed locking implementation using DynamoDB written in Java.

### Description

dynamodb-lock is a distributed locking implementation built upon AWS DynamoDB.

### Usage

First, you will need to create a single table in DynamoDB (the default table name is DYNAMODB_LOCKS). It should have a partition key named "lock_path". 

You may also want to specify a TTL field named "lock_ttl" to DynamoDB to automatically clean up references to any locks that are never explicity released (such as might happen if an application crashes).

The Cloudformation template snippet for this looks as follows:

~~~~
{
  "Type" : "AWS::DynamoDB::Table",
  "Properties" : {
    "AttributeDefinitions" : [ {""} ],
    "KeySchema" : [ KeySchema, ... ],
    "TableName" : "DYNAMODB_LOCKS",
    "TimeToLiveSpecification" : {
      "AttributeName" : "lock_ttl",
      "Enabled" : "true"
    }
  }
}
~~~~

A basic example of using the dynamodb lock:

~~~~
import com.mcconnellsoftware.dynamodblock.DynamoDBLock;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public class Main {
  
  
  public static void main(String[] args) {
    
    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

    DynamoDBLock lock = DynamoDBLock.amazonDynamoDB(client).lockKey("myLock").build();

    try {
      if (lock.acquire()) {
        // Perform critical operations
      }
    } catch (InterruptedException ie) {
      ie.printStackTrace(System.err);
    } finally {
      lock.release();
    }  
  }
}
~~~~


### Requirements

* Java 8
* Amazon Web Services account with DynamoDB access

