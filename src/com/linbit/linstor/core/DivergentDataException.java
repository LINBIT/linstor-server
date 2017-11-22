package com.linbit.linstor.core;

import com.linbit.linstor.LinStorException;

public class DivergentDataException extends LinStorException
{
    private static final long serialVersionUID = 1568799801992745931L;

    public DivergentDataException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText,
        Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }

    public DivergentDataException(
        String message, String descriptionText, String causeText, String correctionText, String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public DivergentDataException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public DivergentDataException(String message)
    {
        super(message);
    }
}
