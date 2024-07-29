package com.linbit.linstor.security;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

/**
 * Thrown to indicate a sign in failure, such as incorrect credentials,
 * a non-existent identity, locked identities, disabled roles, etc.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SignInException extends LinStorException
{
    public SignInException(String message)
    {
        super(message);
    }

    public SignInException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SignInException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public SignInException(
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
