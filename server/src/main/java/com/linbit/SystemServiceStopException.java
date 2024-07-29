package com.linbit;

import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.Nullable;

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

    public SystemServiceStopException(String message, @Nullable Throwable cause)
    {
        super(message, cause);
    }

    public SystemServiceStopException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public SystemServiceStopException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText,
        @Nullable Throwable cause
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, cause);
    }
}
