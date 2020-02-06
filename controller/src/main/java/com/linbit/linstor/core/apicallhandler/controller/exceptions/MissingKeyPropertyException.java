package com.linbit.linstor.core.apicallhandler.controller.exceptions;

import com.linbit.linstor.LinStorException;

public class MissingKeyPropertyException extends LinStorException
{

    public MissingKeyPropertyException(String message)
    {
        super(message);
    }

}
