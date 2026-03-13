package com.linbit.linstor;

import com.linbit.linstor.annotation.Nullable;

public class LinstorParsingException extends LinStorException
{
    private static final long serialVersionUID = -8736964267282722243L;

    public LinstorParsingException(String messageRef)
    {
        super(messageRef);
    }

    public LinstorParsingException(
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

    public LinstorParsingException(
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

    public LinstorParsingException(
        String messageRef,
        @Nullable String descriptionTextRef,
        @Nullable String causeTextRef,
        @Nullable String correctionTextRef,
        @Nullable String detailsTextRef
    )
    {
        super(messageRef, descriptionTextRef, causeTextRef, correctionTextRef, detailsTextRef);
    }

    public LinstorParsingException(String messageRef, @Nullable Throwable causeRef)
    {
        super(messageRef, causeRef);
    }
}
