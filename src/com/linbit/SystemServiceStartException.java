package com.linbit;

import com.linbit.drbdmanage.DrbdManageException;

/**
 * Thrown to indicate that a service failed to start
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SystemServiceStartException extends DrbdManageException
{
    public SystemServiceStartException(String message)
    {
        super(message);
    }

    public SystemServiceStartException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SystemServiceStartException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public SystemServiceStartException(
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
