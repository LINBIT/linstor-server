package com.linbit.drbdmanage.storage;

import com.linbit.drbdmanage.DrbdManageException;

public class StorageException extends DrbdManageException
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
