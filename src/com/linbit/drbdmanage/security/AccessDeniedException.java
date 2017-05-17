package com.linbit.drbdmanage.security;

import com.linbit.drbdmanage.DrbdManageException;

/**
 * Thrown to indicate that an AccessContext does not satisfy the conditions
 * to be allowed access to an object
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class AccessDeniedException extends DrbdManageException
{
    public AccessDeniedException(String message)
    {
        super(message);
    }

    public AccessDeniedException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public AccessDeniedException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public AccessDeniedException(
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
