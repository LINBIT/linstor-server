package com.linbit.drbd.md;

public class PeerCountException extends MdException
{
    public PeerCountException(String message)
    {
        super(message);
    }

    public PeerCountException()
    {
        super("Peer count is out of range");
    }
}
