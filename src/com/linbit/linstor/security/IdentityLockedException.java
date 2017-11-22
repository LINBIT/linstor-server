package com.linbit.linstor.security;

/**
 * Thrown to indicate that an identity entry is locked and cannot be used
 * to sign in
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class IdentityLockedException extends SignInException
{
    public IdentityLockedException(String message)
    {
        super(message);
    }

    public IdentityLockedException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public IdentityLockedException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public IdentityLockedException(
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
