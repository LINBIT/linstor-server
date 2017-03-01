package com.linbit.drbdmanage.security;

/**
 * Thrown to indicate that an AccessContext does not satisfy the conditions
 * to be allowed access to an object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class AccessDeniedException extends Exception
{
    public AccessDeniedException(String message)
    {
        super(message);
    }
}
