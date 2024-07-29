package com.linbit.linstor;

import com.linbit.linstor.annotation.Nullable;

public class LinStorRuntimeException extends RuntimeException implements ErrorContextSupplier
{
    private static final long serialVersionUID = 1475777378033472411L;

    private final @Nullable String excDescription;
    private final @Nullable String excCause;
    private final @Nullable String excCorrection;
    private final @Nullable String excDetails;

    private final @Nullable Long excNumericCode;

    public LinStorRuntimeException(String message)
    {
        this(message, null, null, null, null, null, null);
    }

    public LinStorRuntimeException(String message, @Nullable Throwable cause)
    {
        this(message, null, null, null, null, null, cause);
    }

    public LinStorRuntimeException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText
    )
    {
        this(message, descriptionText, causeText, correctionText, detailsText, null, null);
    }

    public LinStorRuntimeException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText,
        @Nullable Throwable cause
    )
    {
        this(message, descriptionText, causeText, correctionText, detailsText, null, cause);
    }

    public LinStorRuntimeException(
        String message,
        @Nullable String descriptionText,
        @Nullable String causeText,
        @Nullable String correctionText,
        @Nullable String detailsText,
        @Nullable Long numericCode,
        @Nullable Throwable cause
    )
    {
        super(message, cause);
        excDescription = descriptionText;
        excCause = causeText;
        excCorrection = correctionText;
        excDetails = detailsText;
        excNumericCode = numericCode;
    }

    /**
     * Adds the given array of throwables to the Exception's list of suppressed throwable.
     * This method includes all necessary null-checks
     *
     * @param suppressedExceptions
     *
     * @return itself
     */
    public LinStorRuntimeException addSuppressedThrowables(
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
     * Returns a text that describes the problem for which the exception was generated
     *
     * @return Problem description, or null if no such information is available
     */
    public @Nullable String getDescriptionText()
    {
        return excDescription;
    }
    /**
     * Returns the text that describes what caused the problem, for which the exception was generated,
     * to occur
     *
     * @return Problem cause description, or null if no such information is available
     */
    public @Nullable String getCauseText()
    {
        return excCause;
    }

    /**
     * Returns the text that describes possible or recommended resolutions to the problem for
     * which the exception was generated
     *
     * @return Correction instructions, or null if no such information is available
     */
    public @Nullable String getCorrectionText()
    {
        return excCorrection;
    }

    /**
     * Returns the text that contains additional information for error reports
     *
     * @return Additional information, or null if no such information is available
     */
    public @Nullable String getDetailsText()
    {
        return excDetails;
    }

    /**
     * Returns the numeric error code for the problem
     *
     * @return Numeric error code, or null if none has been set
     */
    public @Nullable Long getNumericCode()
    {
        return excNumericCode;
    }

    @Override
    public String getErrorContext()
    {
        StringBuilder sb = new StringBuilder("ErrorContext: ");
        if (excDescription != null)
        {
            sb.append("  Description: ").append(excDescription).append("\n");
        }
        if (excCause != null)
        {
            sb.append("  Cause:       ").append(excCause).append("\n");
        }
        if (excCorrection != null)
        {
            sb.append("  Correction:  ").append(excCorrection).append("\n");
        }
        if (excDetails != null)
        {
            sb.append("  Details:     ").append(excDetails).append("\n");
        }
        if (excNumericCode != null)
        {
            sb.append("  NumericCode:  ").append(excNumericCode).append("\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public boolean hasErrorContext()
    {
        return excDescription != null || excCause != null || excCorrection != null || excDetails != null ||
            excNumericCode != null;
    }
}
