package com.linbit.linstor;

import com.linbit.linstor.annotation.Nullable;

public class LinStorDBRuntimeException extends LinStorRuntimeException
{
    private static final long serialVersionUID = 3691179982631419907L;

    public LinStorDBRuntimeException(String message, @Nullable Throwable cause)
    {
        super(message, cause);
    }

    public LinStorDBRuntimeException(
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

    public LinStorDBRuntimeException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public LinStorDBRuntimeException(String message)
    {
        super(message);
    }
}
