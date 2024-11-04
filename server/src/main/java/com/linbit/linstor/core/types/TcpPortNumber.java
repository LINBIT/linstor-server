package com.linbit.linstor.core.types;

import com.linbit.Checks;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Transmission Control Protocol port number
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class TcpPortNumber implements Comparable<TcpPortNumber>
{
    public static final int PORT_NR_MIN = 1;
    // MagicNumber exception: shift value
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int PORT_NR_MAX = (1 << 16) - 1;

    private static final String PORT_NR_EXC_FORMAT =
        "TCP port number %d is out of range [%d - %d]";

    public final int value;

    public TcpPortNumber(int number) throws ValueOutOfRangeException
    {
        tcpPortNrCheck(number);
        value = number;
    }

    @Override
    public int compareTo(TcpPortNumber other)
    {
        int result = 0;
        if (other == null)
        {
            // null sorts before any existing TcpPortNumber
            result = -1;
        }
        else
        {
            if (value < other.value)
            {
                result = -1;
            }
            else
            if (value > other.value)
            {
                result = 1;
            }
        }
        return result;
    }

    @SuppressFBWarnings("EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS")
    @Override
    public boolean equals(Object other)
    {
        boolean result = false;
        if (this == other)
        {
            result = true;
        }
        else
        if (other != null)
        {
            try
            {
                if (value == ((TcpPortNumber) other).value)
                {
                    result = true;
                }
            }
            catch (ClassCastException castExc)
            {
                if (other instanceof Integer)
                {
                    if (value == (Integer) other)
                    {
                        result = true;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 59 * hash + this.value;
        return hash;
    }

    @Override
    public String toString()
    {
        return Integer.toString(value);
    }

    /**
     * Checks the validity of a TCP port number
     *
     * @param portNr The TCP port number to check
     * @throws ValueOutOfRangeException If the TCP port number is out of range
     */
    public static final void tcpPortNrCheck(int portNr) throws ValueOutOfRangeException
    {
        Checks.genericRangeCheck(portNr, PORT_NR_MIN, PORT_NR_MAX, PORT_NR_EXC_FORMAT);
    }

    public static @Nullable Integer getValueNullable(TcpPortNumber tcpPortNumber)
    {
        return tcpPortNumber == null ? null : tcpPortNumber.value;
    }
}
