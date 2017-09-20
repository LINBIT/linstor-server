package com.linbit.drbdmanage.netcom;

/**
 * Message interface for sending and receiving data
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Message
{
    public byte[] getData()
        throws IllegalMessageStateException;

    public void setData(byte[] data)
        throws IllegalMessageStateException;

    public int getType()
        throws IllegalMessageStateException;
}
