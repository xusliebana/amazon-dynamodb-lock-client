/**
 * Copyright 2013-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * <p>
 * http://aws.amazon.com/asl/
 * <p>
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.dynamodbv2;

import java.util.Objects;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
//import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
//import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import static java.util.stream.Collectors.toList;

/**
 * Lazy-loaded. Not immutable. Not thread safe.
 */
final class LockItemPaginatedQueryIterator extends LockItemPaginatedIterator {
  private final AmazonDynamoDB dynamoDB;
  private volatile QueryRequest queryRequest;
  private final LockItemFactory lockItemFactory;

  /**
   * Initially null to indicate that no pages have been loaded yet.
   * Afterwards, its {@link QueryResponse#lastEvaluatedKey()} is used to tell
   * if there are more pages to load if its
   * {@link QueryResponse#lastEvaluatedKey()} is not null.
   */
  private volatile QueryResult queryResponse = null;

  LockItemPaginatedQueryIterator(final AmazonDynamoDB dynamoDB, final QueryRequest queryRequest, final LockItemFactory lockItemFactory) {
    this.dynamoDB = Objects.requireNonNull(dynamoDB, "dynamoDB must not be null");
    this.queryRequest = Objects.requireNonNull(queryRequest, "queryRequest must not be null");
    this.lockItemFactory = Objects.requireNonNull(lockItemFactory, "lockItemFactory must not be null");
  }

  protected boolean hasAnotherPageToLoad() {
    if (!this.hasLoadedFirstPage()) {
      return true;
    }

    return this.queryResponse.getLastEvaluatedKey() != null && !this.queryResponse.getLastEvaluatedKey().isEmpty();
  }

  protected boolean hasLoadedFirstPage() {
    return this.queryResponse != null;
  }

  protected void loadNextPageIntoResults() {
    this.queryResponse = this.dynamoDB.query(this.queryRequest);

    this.currentPageResults = this.queryResponse.getItems().stream().map(this.lockItemFactory::create).collect(toList());
    this.currentPageResultsIndex = 0;

    this.queryRequest = new QueryRequest().
         withTableName(queryRequest.getTableName()).
         withKeyConditionExpression(queryRequest.getKeyConditionExpression())
        .withExpressionAttributeNames(queryRequest.getExpressionAttributeNames())
        .withExpressionAttributeValues(queryRequest.getExpressionAttributeValues())
        .withExclusiveStartKey(queryResponse.getLastEvaluatedKey());
  }
}
