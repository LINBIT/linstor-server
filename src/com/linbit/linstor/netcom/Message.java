package com.linbit.linstor.netcom;

/**
 * Message interface for sending and receiving data
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Message
{
    byte[] getData()
        throws IllegalMessageStateException;

    void setData(byte[] data)
        throws IllegalMessageStateException;

    int getType()
        throws IllegalMessageStateException;
}
