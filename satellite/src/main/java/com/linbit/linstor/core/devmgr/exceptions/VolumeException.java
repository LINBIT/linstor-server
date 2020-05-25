package com.linbit.linstor.core.devmgr.exceptions;

import com.linbit.linstor.LinStorException;

public class VolumeException extends LinStorException
{
    public VolumeException(
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

    public VolumeException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public VolumeException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public VolumeException(String message)
    {
        super(message);
    }
}
