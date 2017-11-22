package com.linbit.linstor.security;

import com.linbit.linstor.LinStorException;

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
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public SignInException(
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
