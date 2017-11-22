package com.linbit.linstor.security;

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
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public InvalidCredentialsException(
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
