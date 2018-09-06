package com.linbit.linstor.core;

import com.linbit.linstor.LinStorException;

public class PortAlreadyInUseException extends LinStorException
{
    /**
     *
     */
    private static final long serialVersionUID = 740433050178256808L;
    private final int port;

    public PortAlreadyInUseException(int portRef)
    {
        super("The port " + portRef + " is already in use");
        port = portRef;
    }

    public int getPort()
    {
        return port;
    }
}
