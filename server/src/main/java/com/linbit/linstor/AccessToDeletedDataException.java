package com.linbit.linstor;

import com.linbit.linstor.annotation.Nullable;

public class AccessToDeletedDataException extends LinStorRuntimeException
{
    private static final long serialVersionUID = -4216737156323935538L;

    public AccessToDeletedDataException(String message)
    {
        super(message, null);
    }

    public AccessToDeletedDataException(String messageRef, @Nullable Throwable causeRef)
    {
        super(messageRef, causeRef);
    }

    public AccessToDeletedDataException(
        String messageRef,
        @Nullable String descriptionTextRef,
        @Nullable String causeTextRef,
        @Nullable String correctionTextRef,
        @Nullable String detailsTextRef
    )
    {
        super(messageRef, descriptionTextRef, causeTextRef, correctionTextRef, detailsTextRef);
    }

    public AccessToDeletedDataException(
        String messageRef,
        @Nullable String descriptionTextRef,
        @Nullable String causeTextRef,
        @Nullable String correctionTextRef,
        @Nullable String detailsTextRef,
        @Nullable Throwable causeRef
    )
    {
        super(messageRef, descriptionTextRef, causeTextRef, correctionTextRef, detailsTextRef, causeRef);
    }

    public AccessToDeletedDataException(
        String messageRef,
        @Nullable String descriptionTextRef,
        @Nullable String causeTextRef,
        @Nullable String correctionTextRef,
        @Nullable String detailsTextRef,
        @Nullable Long numericCodeRef,
        @Nullable Throwable causeRef
    )
    {
        super(
            messageRef,
            descriptionTextRef,
            causeTextRef,
            correctionTextRef,
            detailsTextRef,
            numericCodeRef,
            causeRef
        );
    }
}
