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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link LockItemPaginatedQueryIterator} that test for
 * {@link LockItemPaginatedQueryIterator#next()}, {@link LockItemPaginatedQueryIterator#hasNext()}
 * and {@link LockItemPaginatedQueryIterator#remove()}
 */
@RunWith(MockitoJUnitRunner.class)
public class LockItemPaginatedQueryIteratorTest extends TestCase {
  @Mock
  AmazonDynamoDBClient dynamodb;
  @Mock
  LockItemFactory factory;

  @Test(expected = UnsupportedOperationException.class)
  public void remove_throwsUnsupportedOperationException() {
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, new QueryRequest(), factory);
    sut.remove();
  }

  @Test(expected = NoSuchElementException.class)
  public void next_whenDoesNotHaveNext_throwsNoSuchElementException() {
    QueryRequest request = new QueryRequest();
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, request, factory);
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    items.add(new HashMap<>());

    QueryResult queryResult1 = new QueryResult();
    queryResult1.setItems(items);
    queryResult1.setCount(1);
    queryResult1.setLastEvaluatedKey(new HashMap<>());

    QueryResult queryResult2 = new QueryResult();
    queryResult2.setItems(items);
    queryResult2.setCount(1);

    when(dynamodb.query(ArgumentMatchers.<QueryRequest>any()))
        .thenReturn(
             queryResult1)
        .thenReturn(queryResult2);
    sut.next();
    sut.next();
  }

  @Test
  public void next_whenMultiplePages_shouldReturnAll() {
    QueryRequest request = new QueryRequest();
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, request, factory);

    assertFalse(sut.hasLoadedFirstPage());

    List<Map<String, AttributeValue>> page1 = new ArrayList<>();
    HashMap<String, AttributeValue> lockItem1 = new HashMap<>();
    page1.add(lockItem1);

    List<Map<String, AttributeValue>> page2 = new ArrayList<>();
    HashMap<String, AttributeValue> lockItem2 = new HashMap<>();
    page2.add(lockItem2);

    HashMap<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
    lastEvaluatedKey.put("has_next", null);
    LockItem mock1 = mock(LockItem.class);
    when(mock1.getOwnerName()).thenReturn("1");
    LockItem mock2 = mock(LockItem.class);
    when(mock2.getOwnerName()).thenReturn("2");

    QueryResult queryResult1 = new QueryResult();
    queryResult1.setItems(page1);
    queryResult1.setCount(1);
    queryResult1.setLastEvaluatedKey(lastEvaluatedKey);

    QueryResult queryResult2 = new QueryResult();
    queryResult2.setItems(page2);
    queryResult2.setCount(1);

    // Single item pages only to simulate multiple pages.
    when(factory.create(any())).thenReturn(mock1).thenReturn(mock2);
    when(dynamodb.query(ArgumentMatchers.<QueryRequest>any()))
        .thenReturn(
            queryResult1)
        .thenReturn(queryResult2);

    LockItem item1 = sut.next();
    assertEquals(item1.getOwnerName(), "1");
    LockItem item2 = sut.next();
    assertEquals(item2.getOwnerName(), "2");
  }

  @Test
  public void next_whenMultipleItemsInOnePage_shouldReturnAll() {
    QueryRequest request = new QueryRequest();
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, request, factory);

    assertFalse(sut.hasLoadedFirstPage());

    List<Map<String, AttributeValue>> page1 = new ArrayList<>();
    HashMap<String, AttributeValue> lockItem1 = new HashMap<>();
    HashMap<String, AttributeValue> lockItem2 = new HashMap<>();
    page1.add(lockItem1);
    page1.add(lockItem2);

    LockItem mock1 = mock(LockItem.class);
    when(mock1.getOwnerName()).thenReturn("1");
    LockItem mock2 = mock(LockItem.class);
    when(mock2.getOwnerName()).thenReturn("2");

    // Multiple items in one page.
    QueryResult queryResult = new QueryResult();
    queryResult.setItems(page1);
    queryResult.setCount(2);

    when(factory.create(any())).thenReturn(mock1).thenReturn(mock2);
    when(dynamodb.query(ArgumentMatchers.<QueryRequest>any()))
        .thenReturn(queryResult);

    LockItem item1 = sut.next();
    assertEquals(item1.getOwnerName(), "1");
    LockItem item2 = sut.next();
    assertEquals(item2.getOwnerName(), "2");
  }

  @Test
  public void hasNext_whenMultiplePages_shouldReturnTrueBeforeLastOne() {
    QueryRequest request = new QueryRequest();
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, request, factory);

    assertFalse(sut.hasLoadedFirstPage());

    List<Map<String, AttributeValue>> page1 = new ArrayList<>();
    HashMap<String, AttributeValue> lockItem1 = new HashMap<>();
    page1.add(lockItem1);

    List<Map<String, AttributeValue>> page2 = new ArrayList<>();
    HashMap<String, AttributeValue> lockItem2 = new HashMap<>();
    page2.add(lockItem2);

    HashMap<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
    lastEvaluatedKey.put("has_next", null);
    LockItem mock1 = mock(LockItem.class);
    LockItem mock2 = mock(LockItem.class);

    QueryResult queryResult1 = new QueryResult();
    queryResult1.setItems(page1);
    queryResult1.setCount(1);
    queryResult1.setLastEvaluatedKey(lastEvaluatedKey);

    QueryResult queryResult2 = new QueryResult();
    queryResult2.setItems(page2);
    queryResult2.setCount(1);

    // Single item pages only to simulate multiple pages.
    when(factory.create(any())).thenReturn(mock1).thenReturn(mock2);
    when(dynamodb.query(ArgumentMatchers.<QueryRequest>any()))
        .thenReturn(queryResult1)
        .thenReturn(queryResult2);

    assertTrue(sut.hasNext());
    sut.next();
    assertTrue(sut.hasNext());
    sut.next();
    assertFalse(sut.hasNext());
  }

  @Test
  public void hasNext_whenMultipleItemsInOnePage_shouldReturnTrueBeforeLastOne() {
    QueryRequest request = new QueryRequest();
    LockItemPaginatedQueryIterator
        sut = new LockItemPaginatedQueryIterator(dynamodb, request, factory);

    assertFalse(sut.hasLoadedFirstPage());

    List<Map<String, AttributeValue>> page1 = new ArrayList<>();
    HashMap<String, AttributeValue> lockItem1 = new HashMap<>();
    HashMap<String, AttributeValue> lockItem2 = new HashMap<>();
    page1.add(lockItem1);
    page1.add(lockItem2);

    LockItem mock1 = mock(LockItem.class);
    LockItem mock2 = mock(LockItem.class);

    QueryResult queryResult1 = new QueryResult();
    queryResult1.setItems(page1);
    queryResult1.setCount(2);


    // Multiple items in one page.
    when(factory.create(any())).thenReturn(mock1).thenReturn(mock2);
    when(dynamodb.query(ArgumentMatchers.<QueryRequest>any()))
        .thenReturn(queryResult1);

    assertTrue(sut.hasNext());
    sut.next();
    assertTrue(sut.hasNext());
    sut.next();
    assertFalse(sut.hasNext());
  }

}
