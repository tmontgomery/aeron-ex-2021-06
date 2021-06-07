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

package app;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.samples.SampleConfiguration;
import io.aeron.samples.SamplesUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.YieldingIdleStrategy;
import support.ExUtil;

import java.util.concurrent.atomic.AtomicBoolean;

// TODO:
// start media driver
// start RateSubscriber
// start SlowSubscriber
// start StreamingPublisher
public class SlowSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    // TODO: try adding tether=false
    private static final String CHANNEL = SampleConfiguration.CHANNEL + "|alias=SlowSubscriber";

    public static void main(final String[] args)
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream id " + STREAM_ID);

        final Aeron.Context ctx = new Aeron.Context()
            .availableImageHandler(SamplesUtil::printAvailableImage)
            .unavailableImageHandler(SamplesUtil::printUnavailableImage);

        final AtomicBoolean running = new AtomicBoolean(true);

        SigInt.register(() ->
        {
            running.set(false);
        });

        try (Aeron aeron = Aeron.connect(ctx);
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            final IdleStrategy idleStrategy = new YieldingIdleStrategy();
            final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) ->
            {
                // TODO: play with this value. Try 100. Try 10. Then take it out
                ExUtil.sleepMilliseconds(1);
            });

            while (running.get())
            {
                final int fragmentsRead = subscription.poll(assembler, FRAGMENT_COUNT_LIMIT);
                idleStrategy.idle(fragmentsRead);
            }

            System.out.println("Shutting down...");
        }
    }
}
