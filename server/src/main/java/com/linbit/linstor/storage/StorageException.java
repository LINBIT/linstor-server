package com.linbit.linstor.storage;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

public class StorageException extends LinStorException
{
    public StorageException(String message)
    {
        super(message);
    }

    public StorageException(String message, @Nullable Exception nestedException)
    {
        super(message, nestedException);
    }

    public StorageException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public StorageException(
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
}
