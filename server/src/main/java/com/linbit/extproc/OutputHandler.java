package com.linbit.extproc;

import java.io.IOException;

public interface OutputHandler extends Runnable
{

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
    byte[] getData() throws IOException;

    /**
     * Waits for I/O completion and availability of all data
     *
     * A waiting thread can be interrupted to unblock a wait
     * in this method
     */
    void finish();
}
