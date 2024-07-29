package com.linbit.linstor.core.apicallhandler.controller.exceptions;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

public class IllegalStorageDriverException extends LinStorException
{
    private static final long serialVersionUID = 5257817958772102903L;

    public IllegalStorageDriverException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText,
        @Nullable Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }

    public IllegalStorageDriverException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public IllegalStorageDriverException(String message, @Nullable Throwable cause)
    {
        super(message, cause);
    }

    public IllegalStorageDriverException(String message)
    {
        super(message);
    }
}
