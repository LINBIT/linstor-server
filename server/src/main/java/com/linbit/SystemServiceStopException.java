package com.linbit;

import com.linbit.linstor.LinStorException;

/**
 * Thrown to indicate that a service failed to stop
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class SystemServiceStopException extends LinStorException
{
    public SystemServiceStopException(String message)
    {
        super(message);
    }

    public SystemServiceStopException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SystemServiceStopException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public SystemServiceStopException(
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
