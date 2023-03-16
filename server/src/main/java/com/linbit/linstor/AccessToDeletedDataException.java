package com.linbit.linstor;

public class AccessToDeletedDataException extends LinStorRuntimeException
{
    private static final long serialVersionUID = -4216737156323935538L;

    public AccessToDeletedDataException(String message)
    {
        super(message, null);
    }

    public AccessToDeletedDataException(String messageRef, Throwable causeRef)
    {
        super(messageRef, causeRef);
    }

    public AccessToDeletedDataException(
        String messageRef,
        String descriptionTextRef,
        String causeTextRef,
        String correctionTextRef,
        String detailsTextRef
    )
    {
        super(messageRef, descriptionTextRef, causeTextRef, correctionTextRef, detailsTextRef);
    }

    public AccessToDeletedDataException(
        String messageRef,
        String descriptionTextRef,
        String causeTextRef,
        String correctionTextRef,
        String detailsTextRef,
        Throwable causeRef
    )
    {
        super(messageRef, descriptionTextRef, causeTextRef, correctionTextRef, detailsTextRef, causeRef);
    }

    public AccessToDeletedDataException(
        String messageRef,
        String descriptionTextRef,
        String causeTextRef,
        String correctionTextRef,
        String detailsTextRef,
        Long numericCodeRef,
        Throwable causeRef
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
