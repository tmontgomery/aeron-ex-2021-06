/*
 * Copyright 2021 StoneTor, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package exercises;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import support.ExUtil;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Ex1Test
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:20121";
    private static final int STREAM_ID = 1001;
    private static final int EXPECTED_NUMBER_OF_MESSAGES = 100;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
    private AtomicInteger numberOfSentMessages = new AtomicInteger(0);

    @Test
    @Disabled("find a solution")
    public void shouldOfferAndPollMessages()
    {
        try (MediaDriver driver = ExUtil.startMediaDriver();
            Aeron aeron = Aeron.connect();
            Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            final AtomicBoolean running = new AtomicBoolean(true);
            final MutableInteger numberOfReceivedMessages = new MutableInteger(0);

            final Thread pubThread = ExUtil.createThread(running, () ->
                sendMessage(publication), new SleepingMillisIdleStrategy(100));

            pubThread.start();

            // TODO: complete handler that checks for numberOfReceivedMessages reaching EXPECTED_NUMBER_OF_MESSAGES
            //  and set running to false.
            final FragmentHandler handler = (buffer, offset, length, header) ->
            {
                running.lazySet(false);
            };

            final IdleStrategy idleStrategy = new YieldingIdleStrategy();
            while (running.get())
            {
                idleStrategy.idle(subscription.poll(handler, 1));
            }

            ExUtil.joinThread(pubThread);

            System.out.println("Sent: " + numberOfSentMessages);
            System.out.println("Received: " + numberOfReceivedMessages);
            Assertions.assertEquals(numberOfReceivedMessages.get(), EXPECTED_NUMBER_OF_MESSAGES);
        }
    }

    private long sendMessage(final Publication publication)
    {
        numberOfSentMessages.getAndAdd(1);
        final long result = publication.offer(srcBuffer);

        return result;
    }
}
