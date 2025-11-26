package com.linbit.exceptions;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

public class InvalidSizeException extends LinStorException
{
    private static final long serialVersionUID = -4155356569792311601L;

    public InvalidSizeException(String messageRef, @Nullable Throwable causeRef)
    {
        super(messageRef, causeRef);
    }
}
