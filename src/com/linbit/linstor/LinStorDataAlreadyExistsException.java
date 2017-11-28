package com.linbit.linstor;

public class LinStorDataAlreadyExistsException extends LinStorException
{
    private static final long serialVersionUID = -996556995066979100L;

    public LinStorDataAlreadyExistsException(
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

    public LinStorDataAlreadyExistsException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public LinStorDataAlreadyExistsException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public LinStorDataAlreadyExistsException(String message)
    {
        super(message);
    }

}
