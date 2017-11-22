package com.linbit.drbd;

import com.linbit.linstor.LinStorException;

/**
 * Indicates failure to modify DRBD's state
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DrbdException extends LinStorException
{
    public DrbdException(String message)
    {
        super(message);
    }

    public DrbdException(String message, Exception nestedException)
    {
        super(message, nestedException);
    }

    public DrbdException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        super(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public DrbdException(
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
