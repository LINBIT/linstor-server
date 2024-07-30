package com.linbit.extproc;

import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.MDC;

/**
 * Logs and saves the output of external commands
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class OutputReceiver implements OutputHandler
{
    // Buffer size 64 kiB
    public static final int OF_BUFFER_SIZE = 0x10000;

    // Initial data buffer size 64 kiB
    public static final int INIT_DATA_SIZE = 0x10000;

    // Data buffer size increment 512 kiB
    public static final int DATA_SIZE_INC = 0x80000;

    // Maximum data size 4 MiB
    public static final int MAX_DATA_SIZE = 0x400000;

    public static final int EOF = -1;

    private final InputStream dataIn;
    private byte[] data;
    private int dataSize;

    private boolean finished;
    private boolean overflow;

    private IOException savedIoExc;
    private final ErrorReporter errLog;
    private final boolean logExecution;
    private final String logId;

    /**
     * Creates a new instance that reads from the specified InputStream
     *
     * @param in InputStream to read data from
     */
    public OutputReceiver(InputStream in, ErrorReporter errLogRef, String logIdRef)
    {
        this(in, errLogRef, true, logIdRef);
    }

    public OutputReceiver(InputStream in, ErrorReporter errLogRef, boolean logExecutionRef, String logIdRef)
    {
        dataIn = in;
        errLog = errLogRef;
        logExecution = logExecutionRef;
        data = new byte[INIT_DATA_SIZE];
        dataSize = 0;
        finished = false;
        overflow = false;
        logId = logIdRef;

        savedIoExc = null;
    }

    /**
     * Read data until end of stream or until the amount of data has
     * reached MAX_DATA_SIZE
     */
    @Override
    public void run()
    {
        try (var ignore = MDC.putCloseable(ErrorReporter.LOGID, logId))
        {
            int lineOffset = 0;
            int lastSearchPos = 0;
            int readCount;
            do
            {
                // Check whether the buffer is full
                if (dataSize >= data.length)
                {
                    // If the buffer is full, try to enlarge it
                    if (!enlargeDataBuffer())
                    {
                        // If the buffer is at its maximum size, but there is still data
                        // to read, set the overflow flag and abort reading
                        if (dataIn.read() != -1)
                        {
                            overflow = true;
                        }
                        break;
                    }
                }
                // Block until at least one byte can be read
                readCount = dataIn.read(data, dataSize, 1);
                if (readCount != EOF)
                {
                    dataSize += readCount;
                    // Check how much more data can be read without blocking
                    int readSize = dataIn.available();
                    if (readSize > 0)
                    {
                        // If the data would not fit into the buffer, read only until reaching
                        // the end of the buffer and let the next iteration take care of
                        // enlarging the buffer or aborting due to overflow
                        if (data.length - dataSize < readSize)
                        {
                            readSize = data.length - dataSize;
                        }
                        // readSize may be 0, this case is handled correctly by read()
                        readCount = dataIn.read(data, dataSize, readSize);
                        // readCount should never be EOF here, but double-check for good measure
                        if (readCount != EOF)
                        {
                            dataSize += readCount;
                        }
                    }
                }
                // Log any completed lines
                lineOffset = logLines(lineOffset, lastSearchPos);
                lastSearchPos = dataSize;
            }
            while (readCount != -1);

            // If there is more data than MAX_DATA_SIZE, read and discard
            // the rest of the data to avoid blocking a child process that
            // pipes data to this instance
            if (overflow)
            {
                byte[] ofBuffer = new byte[OF_BUFFER_SIZE];
                do
                {
                    readCount = dataIn.read(ofBuffer, 0, ofBuffer.length);
                }
                while (readCount != -1);
            }
        }
        catch (IOException ioExc)
        {
            data = null;
            savedIoExc = ioExc;
        }
        finally
        {
            // Put the data into a new buffer of exactly the size of
            // the data's length, unless the data already fills all
            // of the current buffer
            if (data != null)
            {
                if (dataSize != data.length)
                {
                    byte[] srcData = data;
                    data = new byte[dataSize];
                    System.arraycopy(srcData, 0, data, 0, dataSize);
                }
            }
            // Notify all waiting threads that the data is ready
            // for use
            synchronized (this)
            {
                finished = true;
                notifyAll();
            }
        }
    }

    /**
     * Returns the data that has been read as a byte array
     *
     * Make sure that the data is available by calling finish() before
     * the first call of getData().
     *
     * @return byte array containing the data
     * @throws IOException If the data did not fit into the maximum capacity of
     *     this instance's buffer, if an IOException was encountered while
     *     reading the data, or if I/O on the data is still in progress
     */
    @Override
    public byte[] getData() throws IOException
    {
        // If I/O on the data is unfinished, generate an IOException
        if (!finished)
        {
            throw new IOException("Attempt to access data before I/O is finished");
        }
        // Generate an IOException if the data did not fit into the buffer
        if (overflow)
        {
            throw new IOException("Data buffer size limit exceeded");
        }
        // If there is a saved IOException, throw it now
        if (savedIoExc != null)
        {
            throw savedIoExc;
        }
        return data;
    }

    /**
     * Waits for I/O completion and availability of all data
     *
     * A waiting thread can be interrupted to unblock a wait
     * in this method
     */
    @Override
    public void finish()
    {
        synchronized (this)
        {
            try
            {
                while (!finished)
                {
                    wait();
                }
            }
            catch (InterruptedException intrExc)
            {
                // Thread may be interrupted to unblock the wait
            }
        }
    }

    /**
     * Increases the size of the data buffer unless it has already
     * reached a size of MAX_DATA_SIZE
     *
     * @return true if the buffer size was increased
     */
    private boolean enlargeDataBuffer()
    {
        boolean resized = false;
        if (data.length < MAX_DATA_SIZE)
        {
            int newSize = MAX_DATA_SIZE - data.length >= DATA_SIZE_INC ?
                          data.length + DATA_SIZE_INC : MAX_DATA_SIZE;
            byte[] newBuffer = new byte[newSize];
            System.arraycopy(data, 0, newBuffer, 0, dataSize);
            data = newBuffer;
            resized = true;
        }
        return resized;
    }

    /**
     * Log all text lines that are available so far and that have not
     * previously been logged
     *
     * @param lineOffset Start offset of the current line
     * @param lastSearchPos Position where the last search for new lines left off
     * @return new lineOffset for the next call of this method
     */
    private int logLines(final int lineOffset, final int lastSearchPos)
    {
        int updLineOffset = lineOffset;
        if (logExecution)
        {
            for (int idx = lastSearchPos; idx < dataSize; ++idx)
            {
                if (data[idx] == '\n')
                {
                    String logString = new String(data, updLineOffset, idx - updLineOffset);
                    errLog.logTrace("%s", logString);
                    updLineOffset = idx + 1;
                }
            }
        }
        return updLineOffset;
    }
}
