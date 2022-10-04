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

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for AmazonDynamoDBLockClientOptions.
 *
 * @author <a href="mailto:amcp@amazon.com">Alexander Patrikalakis</a> 2017-07-13
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({AmazonDynamoDBLockClientTest.class, AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder.class})
public class AmazonDynamoDBLockClientOptionsTest {
    AmazonDynamoDB dynamodb = PowerMockito.mock(AmazonDynamoDB.class);

    @Test
    public void testBuilder_whenGetLocalHostThrowsUnknownHostException_uuidCreateRandomIsCalled() throws UnknownHostException, InterruptedException {
        final UUID uuid = AmazonDynamoDBLockClientTest.setOwnerNameToUuid();
        AmazonDynamoDBLockClientOptions.AmazonDynamoDBLockClientOptionsBuilder builder = AmazonDynamoDBLockClientOptions.builder(dynamodb, "table")
            .withLeaseDuration(2L)
            .withHeartbeatPeriod(1L)
            .withTimeUnit(TimeUnit.SECONDS)
            .withPartitionKeyName("customer");
        System.out.println(builder.toString());
        //verifyStatic();

        AmazonDynamoDBLockClientOptions options = builder.build();
        AmazonDynamoDBLockClient client = new AmazonDynamoDBLockClient(options);
        Map<String, AttributeValue> previousLockItem = new HashMap<>(3);
        previousLockItem.put("ownerName", new AttributeValue("foobar"));
        previousLockItem.put("recordVersionNumber", new AttributeValue("oolala"));
        previousLockItem.put("leaseDuration", new AttributeValue("1"));
        when(dynamodb.getItem(Matchers.<GetItemRequest>any())).thenReturn(new GetItemResult().withItem(previousLockItem));
        LockItem lock = client.acquireLock(AcquireLockOptions.builder("asdf").build());
        assertEquals(uuid.toString(), lock.getOwnerName());
    }
}
