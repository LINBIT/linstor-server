package com.linbit.linstor;

import javax.annotation.Nullable;

/**
 * Provides an Exception base class that allows for detailed reporting of the
 * cause, possible correction and additional details of a problem.
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class LinStorException extends Exception
{
    private String excDescription;
    private String excCause;
    private String excCorrection;
    private String excDetails;

    private Long excNumericCode;

    public LinStorException(String message)
    {
        super(message);
    }

    public LinStorException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public LinStorException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText
    )
    {
        this(message, descriptionText, causeText, correctionText, detailsText, null);
    }

    public LinStorException(
        String message,
        String descriptionText,
        String causeText,
        String correctionText,
        String detailsText,
        Throwable cause
    )
    {
        super(message, cause);
        excDescription = descriptionText;
        excCause = causeText;
        excCorrection = correctionText;
        excDetails = detailsText;
    }

    /**
     * Adds the given array of throwables to the Exception's list of suppressed throwable.
     * This method includes all necessary null-checks
     *
     * @param suppressedExceptions
     *
     * @return itself
     */
    public LinStorException addSuppressedThrowables(
        @Nullable Throwable... suppressedExceptions
    )
    {
        if (suppressedExceptions != null)
        {
            for (Throwable exc : suppressedExceptions)
            {
                if (exc != null)
                {
                    addSuppressed(exc);
                }
            }
        }
        return this;
    }

    /**
     * Sets the text that describes the problem for which the exception was generated
     *
     * @param text Problem description, or null to clear an existing problem description
     */
    public void setDescriptionText(String text)
    {
        excDescription = text;
    }

    /**
     * Sets the text that describes what caused the problem, for which the exception was generated,
     * to occur
     *
     * @param text Cause description, or null to clear an existing cause description
     */
    public void setCauseText(String text)
    {
        excCause = text;
    }

    /**
     * Sets the text that describes possible or recommended resolutions to the problem for
     * which the exception was generated
     *
     * @param text Correction instructions, or null to clear existing correction instructions
     */
    public void setCorrectionText(String text)
    {
        excCorrection = text;
    }

    /**
     * Sets the text that contains additional information for error reports
     *
     * @param text Additional information, or null to clear existing additional information
     */
    public void setDetailsText(String text)
    {
        excDetails = text;
    }

    /**
     * Returns a text that describes the problem for which the exception was generated
     *
     * @return Problem description, or null if no such information is available
     */
    public String getDescriptionText()
    {
        return excDescription;
    }
    /**
     * Returns the text that describes what caused the problem, for which the exception was generated,
     * to occur
     *
     * @return Problem cause description, or null if no such information is available
     */
    public String getCauseText()
    {
        return excCause;
    }

    /**
     * Returns the text that describes possible or recommended resolutions to the problem for
     * which the exception was generated
     *
     * @return Correction instructions, or null if no such information is available
     */
    public String getCorrectionText()
    {
        return excCorrection;
    }

    /**
     * Returns the text that contains additional information for error reports
     *
     * @return Additional information, or null if no such information is available
     */
    public String getDetailsText()
    {
        return excDetails;
    }

    /**
     * Attaches a numeric error code
     *
     * @param errorCode Numeric error code for the problem being reported by this exception
     */
    public void setNumericCode(Long errorCode)
    {
        excNumericCode = errorCode;
    }

    /**
     * Returns the numeric error code for the problem
     *
     * @return Numeric error code, or null if none has been set
     */
    public Long getNumericCode()
    {
        return excNumericCode;
    }
}
