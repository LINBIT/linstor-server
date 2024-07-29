package com.linbit;

import com.linbit.linstor.annotation.Nullable;

public class ImplementationError extends Error
{
    public ImplementationError(String message)
    {
        super(message);
    }

    public ImplementationError(Throwable cause)
    {
        super(cause);
    }

    public ImplementationError(String message, @Nullable Throwable cause)
    {
        super(message, cause);
    }
}
