package com.linbit.drbdmanage.security;

/**
 * Thrown to indicate a sign in failure, such as incorrect credentials,
 * a non-existent identity, locked identities, disabled roles, etc.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SignInException extends Exception
{
    public SignInException(String message)
    {
        super(message);
    }

    public SignInException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SignInException(Throwable cause)
    {
        super(cause);
    }
}
