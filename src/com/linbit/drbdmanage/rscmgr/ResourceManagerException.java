package com.linbit.drbdmanage.rscmgr;

import com.linbit.drbdmanage.DrbdManageException;

/**
 * Provides an Exception base class for all resource management-related errors
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ResourceManagerException extends DrbdManageException
{
    public ResourceManagerException(String message)
    {
        super(message);
    }

    public ResourceManagerException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ResourceManagerException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText);
    }

    public ResourceManagerException(
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
