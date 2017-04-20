package com.linbit.drbdmanage.security;

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

    public IdentityLockedException(String message, Exception cause)
    {
        super(message, cause);
    }

    public IdentityLockedException(Exception cause)
    {
        super(cause);
    }
}
