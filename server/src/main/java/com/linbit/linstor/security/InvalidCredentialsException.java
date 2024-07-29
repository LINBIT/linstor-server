package com.linbit.linstor.security;

import com.linbit.linstor.annotation.Nullable;

/**
 * Thrown to indicate an invalid combination of identity name and password
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class InvalidCredentialsException extends SignInException
{
    public InvalidCredentialsException(String message)
    {
        super(message);
    }

    public InvalidCredentialsException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public InvalidCredentialsException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public InvalidCredentialsException(
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
