package com.linbit.linstor.storage;

import com.linbit.linstor.LinStorException;

public class StorageException extends LinStorException
{
    public StorageException(String message)
    {
        super(message);
    }

    public StorageException(String message, Exception nestedException)
    {
        super(message, nestedException);
    }

    public StorageException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public StorageException(
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
}
