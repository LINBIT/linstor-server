package com.linbit.drbdmanage.propscon;

import java.sql.SQLException;

/**
 * Serial number generator
 *
 * IMPORTANT:
 * See the implementation classes' documentation for information
 * on synchronization requirements.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface SerialGenerator
{
    public static final String KEY_SERIAL = "serial";

    /**
     * Returns the most recent serial number without opening a new generation
     * of information.
     *
     * @return Serial number
     */
    public long peekSerial() throws SQLException;

    /**
     * Returns the serial number for a new generation of information. Multiple calls
     * of this method return the same serial number until the active generation is
     * closed by calling closeGeneration().
     *
     * @return New generation serial number
     * @throws SQLException
     */
    public long newSerial() throws SQLException;

    /**
     * Closes a generation of information. Calls of newSerial() that are made after
     * this method returns will return a serial number that differs from the serial
     * number of the current generation of information.
     */
    public void closeGeneration();
}
