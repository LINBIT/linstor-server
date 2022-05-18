package com.linbit.extproc;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingDeque;

public class OutputProxy implements Runnable
{
    public interface Event // marker interface
    {
    }

    public static final int EOF = -1;
    /** 1MiB */
    public static final int DFLT_BUFFER_SIZE = 1 << 20;

    protected final InputStream dataIn;
    protected final BlockingDeque<Event> deque;
    protected final boolean useOut;
    protected boolean shutdown;

    private int bufferSize;

    public OutputProxy(
        final InputStream dataInRef,
        final BlockingDeque<Event> dequeRef,
        final boolean useOutRef
    )
    {
        this(dataInRef, dequeRef, useOutRef, DFLT_BUFFER_SIZE);
    }

    public OutputProxy(
        final InputStream dataInRef,
        final BlockingDeque<Event> dequeRef,
        final boolean useOutRef,
        final int bufferSizeRef
    )
    {
        dataIn = dataInRef;
        deque = dequeRef;
        useOut = useOutRef;
        bufferSize = bufferSizeRef;

        shutdown = false;
    }

    @Override
    public void run()
    {
        int read = 0;
        byte[] data = new byte[bufferSize];
        int dataPos = 0;
        while (read != EOF && !shutdown)
        {
            // First read from the InputStream
            try
            {
                read = dataIn.read(data, dataPos, data.length - dataPos);

                if (read != EOF)
                {
                    dataPos += read;
                    if (dataPos == bufferSize)
                    {
                        addToDeque(data);
                        data = new byte[bufferSize];
                        dataPos = 0;
                    }
                }
                else
                {
                    byte[] trimmedData = new byte[dataPos];
                    System.arraycopy(data, 0, trimmedData, 0, dataPos);
                    addToDeque(trimmedData);
                    addEofToDeque();
                }
            }
            catch (IOException ioExc)
            {
                if (!shutdown)
                {
                    try
                    {
                        addToDeque(ioExc);
                    }
                    catch (InterruptedException exc)
                    {
                        // FIXME: Error reporting required
                        exc.printStackTrace();
                    }
                }
            }
            catch (InterruptedException interruptedExc)
            {
                if (!shutdown)
                {
                    // FIXME: Error reporting required
                    interruptedExc.printStackTrace();
                }
            }
        }
    }

    protected void addEofToDeque() throws InterruptedException
    {
        addToDeque(new EOFEvent(useOut));
    }

    protected void addToDeque(Event eventRef) throws InterruptedException
    {
        deque.put(eventRef);
    }

    protected void addToDeque(byte[] delimitedData) throws InterruptedException
    {
        Event event;
        if (useOut)
        {
            event = new StdOutEvent(delimitedData);
        }
        else
        {
            event = new StdErrEvent(delimitedData);
        }
        addToDeque(event);
    }

    protected void addToDeque(Exception exc) throws InterruptedException
    {
        addToDeque(new ExceptionEvent(exc));
    }

    public void expectShutdown()
    {
        shutdown = true;
    }

    public static class StdOutEvent implements Event
    {
        public final byte[] data;

        public StdOutEvent(byte[] dataRef)
        {
            data = dataRef;
        }
    }

    public static class StdErrEvent implements Event
    {
        public final byte[] data;

        public StdErrEvent(byte[] dataRef)
        {
            data = dataRef;
        }
    }

    public static class ExceptionEvent implements Event
    {
        public final Exception exc;

        public ExceptionEvent(Exception excRef)
        {
            exc = excRef;
        }
    }

    public static class EOFEvent implements Event
    {
        /**
         * True if EOF comes from stdOut stream, false if EOF comes from stdErr stream
         */
        public final boolean stdOut;

        public EOFEvent(boolean useOutRef)
        {
            stdOut = useOutRef;
        }
    }

}
