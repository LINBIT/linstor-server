package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.linstor.LinStorException;

public class NoInitialStateException extends LinStorException
{
    private static final long serialVersionUID = 5900729475006712659L;

    public NoInitialStateException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText,
        Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }

    public NoInitialStateException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public NoInitialStateException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public NoInitialStateException(String message)
    {
        super(message);
    }
}
