package support;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

public class ExUtil
{
    public static MediaDriver startMediaDriver()
    {
        return MediaDriver.launch(
            new MediaDriver.Context()
                .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
                .threadingMode(ThreadingMode.SHARED));
    }

    public static void stopMediaDriver(final MediaDriver driver)
    {
        if (null != driver)
        {
            CloseHelper.close(driver);
            driver.context().deleteDirectory();
        }
    }

    public static Thread createThread(
        final AtomicBoolean running, final LongSupplier doWork, final IdleStrategy idleStrategy)
    {
        return new Thread(() ->
        {
            while (running.get())
            {
                idleStrategy.idle((int)doWork.getAsLong());
            }
        });
    }

    public static void joinThread(final Thread thread)
    {
        try
        {
            if (null != thread)
            {
                thread.join();
            }
        }
        catch (final InterruptedException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public static void sleepMilliseconds(final int millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (final InterruptedException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
