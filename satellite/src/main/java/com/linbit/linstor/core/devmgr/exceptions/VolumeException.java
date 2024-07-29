package com.linbit.linstor.core.devmgr.exceptions;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

public class VolumeException extends LinStorException
{
    public VolumeException(
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

    public VolumeException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public VolumeException(String message, @Nullable Throwable cause)
    {
        super(message, cause);
    }

    public VolumeException(String message)
    {
        super(message);
    }
}
