package com.linbit.linstor.core.apicallhandler.controller.exceptions;

import com.linbit.linstor.LinStorException;

public class IncorrectPassphraseException extends LinStorException
{

    public IncorrectPassphraseException(String message)
    {
        super(message);
    }

}
