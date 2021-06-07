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
import org.agrona.ErrorHandler;
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

import static org.junit.jupiter.api.Assertions.fail;

public class Ex2Test implements ErrorHandler
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:20121";
    private static final int STREAM_ID = 1001;
    private static final int EXPECTED_NUMBER_OF_MESSAGES = 500;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);
    private final AtomicInteger numberOfSentMessages = new AtomicInteger(0);

    @Test
    @Disabled("this will hang as the sending of messages needs to be filled in")
    public void shouldOfferAndPollMessages()
    {
        try (MediaDriver driver = ExUtil.startMediaDriver();
            Aeron aeron = Aeron.connect(new Aeron.Context().subscriberErrorHandler(this));
            Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            final AtomicBoolean running = new AtomicBoolean(true);
            final MutableInteger numberOfReceivedMessages = new MutableInteger(0);

            // TODO: make sure publication sends exactly the right message content
            //  and the right number of messages exactly.

            final Thread pubThread = ExUtil.createThread(running, () ->
                sendMessage(publication), new SleepingMillisIdleStrategy(100));

            pubThread.start();

            final FragmentHandler handler = (buffer, offset, length, header) ->
            {
                Assertions.assertEquals(buffer.getStringAscii(offset), "Hello World! " + numberOfReceivedMessages.get());
                if (numberOfReceivedMessages.addAndGet(1) >= EXPECTED_NUMBER_OF_MESSAGES)
                {
                    running.lazySet(false);
                }
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
            Assertions.assertEquals(numberOfReceivedMessages.get(), numberOfSentMessages.get());
        }
    }

    private long sendMessage(final Publication publication)
    {
        // TODO: you can use this or use your own method
        return 0;
    }

    public void onError(final Throwable throwable)
    {
        fail("onError: " + throwable);
    }
}
