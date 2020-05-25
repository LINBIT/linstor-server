package com.linbit.linstor.core.devmgr.exceptions;

import com.linbit.linstor.LinStorException;

public class ResourceException extends LinStorException
{
    public ResourceException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText,
        Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }

    public ResourceException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public ResourceException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ResourceException(String message)
    {
        super(message);
    }
}
