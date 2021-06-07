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

package solutions;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
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

public class Ex3Part2SolutionTest implements ErrorHandler
{
    private static final String CHANNEL_PUB = "aeron:udp?control=localhost:20121|control-mode=dynamic";
    private static final String CHANNEL_SUB_1 = "aeron:udp?control=localhost:20121|control-mode=dynamic|endpoint=localhost:20122";
    private static final String CHANNEL_SUB_2 = "aeron:udp?control=localhost:20121|control-mode=dynamic|endpoint=localhost:20123";
    private static final int STREAM_ID = 1001;
    private static final int EXPECTED_NUMBER_OF_MESSAGES = 500;

    private final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1600]);
    private final AtomicInteger numberOfSentMessages = new AtomicInteger(0);

    @Test
    @Disabled
    public void shouldOfferAndPollMessages()
    {
        try (MediaDriver driver = ExUtil.startMediaDriver();
            Aeron aeron = Aeron.connect(new Aeron.Context().subscriberErrorHandler(this));
            Publication publication = aeron.addPublication(CHANNEL_PUB, STREAM_ID);
            Subscription subscription1 = aeron.addSubscription(CHANNEL_SUB_1, STREAM_ID);
            Subscription subscription2 = aeron.addSubscription(CHANNEL_SUB_2, STREAM_ID))
        {
            final AtomicBoolean running = new AtomicBoolean(true);
            final MutableInteger numberOfReceivedMessages1 = new MutableInteger(0);
            final MutableInteger numberOfReceivedMessages2 = new MutableInteger(0);

            while (!subscription1.isConnected() && !subscription2.isConnected())
            {
                YieldingIdleStrategy.INSTANCE.idle();
            }

            if (srcBuffer.capacity() > publication.maxPayloadLength())
            {
                System.out.println("WARNING: messages will be fragmented!");
            }

            final Thread pubThread = ExUtil.createThread(running, () ->
                sendMessage(publication), new SleepingMillisIdleStrategy(100));

            pubThread.start();

            final FragmentHandler handler1 = new FragmentAssembler((buffer, offset, length, header) ->
            {
                Assertions.assertEquals(buffer.getStringAscii(offset), "Hello World! " + numberOfReceivedMessages1.get());
                numberOfReceivedMessages1.addAndGet(1);
            });

            final FragmentHandler handler2 = new FragmentAssembler((buffer, offset, length, header) ->
            {
                Assertions.assertEquals(buffer.getStringAscii(offset), "Hello World! " + numberOfReceivedMessages2.get());
                numberOfReceivedMessages2.addAndGet(1);
            });

            final IdleStrategy idleStrategy = new YieldingIdleStrategy();
            while (running.get())
            {
                int workCount = 0;

                workCount += subscription1.poll(handler1, 1);
                workCount += subscription2.poll(handler2, 1);

                if (numberOfReceivedMessages1.get() >= EXPECTED_NUMBER_OF_MESSAGES &&
                    numberOfReceivedMessages2.get() >= EXPECTED_NUMBER_OF_MESSAGES)
                {
                    running.lazySet(false);
                }

                idleStrategy.idle(workCount);
            }

            ExUtil.joinThread(pubThread);

            System.out.println("Sent: " + numberOfSentMessages);
            System.out.println("Received subscription 1: " + numberOfReceivedMessages1);
            System.out.println("Received subscription 2: " + numberOfReceivedMessages2);
            Assertions.assertEquals(numberOfReceivedMessages1.get(), EXPECTED_NUMBER_OF_MESSAGES);
            Assertions.assertEquals(numberOfReceivedMessages1.get(), numberOfSentMessages.get());
            Assertions.assertEquals(numberOfReceivedMessages2.get(), numberOfSentMessages.get());
        }
    }

    private long sendMessage(final Publication publication)
    {
        long workCount = 0;

        if (numberOfSentMessages.get() < EXPECTED_NUMBER_OF_MESSAGES)
        {
            srcBuffer.putStringAscii(0, "Hello World! " + numberOfSentMessages.get());
            long result = publication.offer(srcBuffer);

            if (result > 0)
            {
                numberOfSentMessages.getAndAdd(1);
                workCount += 1;
            }
            else if (Publication.CLOSED == result)
            {
                throw new RuntimeException("Publication closed");
            }
        }

        return workCount;
    }

    public void onError(final Throwable throwable)
    {
        fail("onError: " + throwable);
    }
}
