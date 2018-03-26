package com.linbit.linstor;

public class LinstorEncryptionException extends LinStorException
{
    private static final long serialVersionUID = -5126076573696683377L;

    public LinstorEncryptionException(
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

    public LinstorEncryptionException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public LinstorEncryptionException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public LinstorEncryptionException(String message)
    {
        super(message);
    }


}
