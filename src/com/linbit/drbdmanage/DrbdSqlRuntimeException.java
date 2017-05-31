package com.linbit.drbdmanage;

public class DrbdSqlRuntimeException extends DrbdManageRuntimeException
{
    private static final long serialVersionUID = 3691179982631419907L;

    public DrbdSqlRuntimeException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public DrbdSqlRuntimeException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText,
        Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }

    public DrbdSqlRuntimeException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public DrbdSqlRuntimeException(String message)
    {
        super(message);
    }
}
