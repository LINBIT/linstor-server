package com.linbit.linstor;

public class LinStorDBRuntimeException extends LinStorRuntimeException
{
    private static final long serialVersionUID = 3691179982631419907L;

    public LinStorDBRuntimeException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public LinStorDBRuntimeException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText,
        Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }

    public LinStorDBRuntimeException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public LinStorDBRuntimeException(String message)
    {
        super(message);
    }
}
