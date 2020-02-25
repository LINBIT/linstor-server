package com.linbit.linstor.logging;

import java.util.Date;

public class ErrorReport extends LinstorFile {
    public ErrorReport(final String nodeNameRef, final String fileNameRef, Date dateRef, final String textRef)
    {
        super(nodeNameRef, fileNameRef, dateRef, textRef);
    }
}
