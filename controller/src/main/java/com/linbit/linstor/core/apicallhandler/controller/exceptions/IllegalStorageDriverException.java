package com.linbit.linstor.core.apicallhandler.controller.exceptions;

import com.linbit.linstor.LinStorException;

public class IllegalStorageDriverException extends LinStorException
{
    private static final long serialVersionUID = 5257817958772102903L;

    public IllegalStorageDriverException(
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

    public IllegalStorageDriverException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public IllegalStorageDriverException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public IllegalStorageDriverException(String message)
    {
        super(message);
    }
}
