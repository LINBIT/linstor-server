package com.linbit.linstor;

public class LinStorSqlRuntimeException extends LinStorRuntimeException
{
    private static final long serialVersionUID = 3691179982631419907L;

    public LinStorSqlRuntimeException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public LinStorSqlRuntimeException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText,
        Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }

    public LinStorSqlRuntimeException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public LinStorSqlRuntimeException(String message)
    {
        super(message);
    }
}
