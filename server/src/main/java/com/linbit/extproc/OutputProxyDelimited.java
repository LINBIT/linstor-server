package com.linbit.extproc;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingDeque;

public class OutputProxyDelimited extends OutputProxy
{
    // Initial data buffer size 64 kiB
    public static final int INIT_DATA_SIZE = 0x10000;

    // Data buffer size increment 512 kiB
    public static final int DATA_SIZE_INC = 0x80000;

    // Maximum data size 4 MiB
    public static final int MAX_DATA_SIZE = 0x400000;

    private byte[] data;
    private int dataPos;
    private int dataLimit;

    private final byte delimiter;
    public OutputProxyDelimited(
        final InputStream in,
        final BlockingDeque<Event> dequeRef,
        final byte delimiterPrm,
        final boolean useOutPrm
    )
    {
        super(in, dequeRef, useOutPrm);
        delimiter = delimiterPrm;
        data = new byte[INIT_DATA_SIZE];
        dataPos = 0;
        dataLimit = 0;
    }

    @Override
    public void run()
    {
        int read = 0;
        while (read != EOF && !shutdown)
        {
            // First read from the InputStream
            try
            {
                read = dataIn.read(data, dataPos, data.length - dataPos);

                if (read != EOF)
                {
                    dataLimit += read;
                    // Search for the delimiter starting from dataPos
                    while (dataPos < dataLimit)
                    {
                        if (data[dataPos] == delimiter)
                        {
                            // Put the found data into the deque
                            byte[] delimitedData = new byte[dataPos];
                            System.arraycopy(data, 0, delimitedData, 0, dataPos);
                            addToDeque(delimitedData);

                            // Skip the delimiter
                            dataPos += 1;

                            if (dataPos == dataLimit)
                            {
                                // no need to copy, all data will be overridden anyways
                                dataLimit = 0;
                            }
                            else
                            {
                                // Copy all remaining data to the start of our array
                                System.arraycopy(data, dataPos, data, 0, dataLimit - dataPos);
                                dataLimit -= dataPos;
                            }
                            dataPos = 0;
                        }
                        else
                        {
                            ++dataPos;
                        }
                    }

                    if (dataLimit == data.length)
                    {
                        if (dataLimit < MAX_DATA_SIZE)
                        {
                            byte[] enlarged = new byte[data.length + DATA_SIZE_INC];
                            System.arraycopy(data, 0, enlarged, 0, data.length);
                            data = enlarged;
                        }
                        else
                        {
                            addToDeque(new MaxBufferSizeReached());
                        }
                    }
                }
                else
                {
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

    public static class MaxBufferSizeReached extends Exception
    {
        private static final long serialVersionUID = 3479687941054839688L;
    }
}
