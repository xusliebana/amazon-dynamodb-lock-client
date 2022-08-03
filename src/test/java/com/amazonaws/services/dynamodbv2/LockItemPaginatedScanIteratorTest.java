/**
 * Copyright 2013-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for LockItemPaginatedScanIterator.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(MockitoJUnitRunner.class)
public class LockItemPaginatedScanIteratorTest {
    @Mock
    AmazonDynamoDB dynamodb;
    @Mock
    LockItemFactory factory;

    @Test(expected = UnsupportedOperationException.class)
    public void remove_throwsUnsupportedOperationException() {
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, new ScanRequest(), factory);
        sut.remove();
    }

    @Test(expected = NoSuchElementException.class)
    public void next_whenDoesNotHaveNext_throwsNoSuchElementException() {
        ScanRequest request = new ScanRequest();
        LockItemPaginatedScanIterator sut = new LockItemPaginatedScanIterator(dynamodb, request, factory);
        List<Map<String, AttributeValue>> list1 = new ArrayList<>();
        list1.add(new HashMap<>());
        when(dynamodb.scan(ArgumentMatchers.<ScanRequest>any()))
            .thenReturn(new ScanResult().withItems(list1).withCount(1).withLastEvaluatedKey(new HashMap<>()))
            .thenReturn(new ScanResult().withItems(Collections.emptyList()).withCount(0));
        sut.next();
        sut.next();
    }
}
