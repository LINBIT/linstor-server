package com.linbit.drbdmanage.propscon;

import com.linbit.ErrorCheck;
import com.linbit.ImplementationError;

/**
 * A sequential serial number generator implementation
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SeqSerialGenerator implements SerialGenerator
{
    private SerialAccessor accessor;
    private boolean changeOpen;

    private final Object serialMutex;

    SeqSerialGenerator(SerialAccessor acc)
    {
        ErrorCheck.ctorNotNull(SeqSerialGenerator.class, SerialAccessor.class, acc);
        accessor = acc;
        changeOpen = false;
        serialMutex = new Object();
    }

    /**
     * Returns the most recent serial number without opening a new generation
     * of information.
     *
     * @return Serial number
     */
    @Override
    public long peekSerial()
    {
        return accessor.getSerial();
    }

    /**
     * Returns the serial number for a new generation of information. Multiple calls
     * of this method return the same serial number until the active generation is
     * closed by calling closeGeneration().
     *
     * @return New generation serial number
     */
    @Override
    public long newSerial()
    {
        long serial = 0;
        synchronized (serialMutex)
        {
            serial = accessor.getSerial();
            if (!changeOpen)
            {
                changeOpen = true;
                ++serial;
                accessor.setSerial(serial);
            }
        }
        return serial;
    }

    /**
     * Closes a generation of information. Calls of newSerial() that are made after
     * this method returns will return a serial number that differs from the serial
     * number of the current generation of information.
     */
    @Override
    public void closeGeneration()
    {
        synchronized (serialMutex)
        {
            changeOpen = false;
        }
    }
}
