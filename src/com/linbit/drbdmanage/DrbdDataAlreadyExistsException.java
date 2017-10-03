package com.linbit.drbdmanage;

public class DrbdDataAlreadyExistsException extends DrbdManageRuntimeException
{
    private static final long serialVersionUID = -996556995066979100L;

    public DrbdDataAlreadyExistsException(
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

    public DrbdDataAlreadyExistsException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public DrbdDataAlreadyExistsException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public DrbdDataAlreadyExistsException(String message)
    {
        super(message);
    }

}
