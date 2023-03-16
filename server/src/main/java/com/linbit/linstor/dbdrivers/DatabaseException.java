package com.linbit.linstor.dbdrivers;

import com.linbit.linstor.LinStorException;

public class DatabaseException extends LinStorException
{
    private static final long serialVersionUID = -8772770979952745641L;

    public DatabaseException(Throwable cause)
    {
        super("DatabaseException", cause);
    }

    public DatabaseException(
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

    public DatabaseException(
        String messageRef,
        String descriptionTextRef,
        String causeTextRef,
        String correctionTextRef,
        String detailsTextRef
    )
    {
        super(messageRef, descriptionTextRef, causeTextRef, correctionTextRef, detailsTextRef);
    }

    public DatabaseException(String messageRef, Throwable causeRef)
    {
        super(messageRef, causeRef);
    }

    public DatabaseException(String messageRef)
    {
        super(messageRef);
    }
}
