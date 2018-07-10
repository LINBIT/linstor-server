package com.linbit.linstor.core.apicallhandler.response;

import java.sql.SQLException;

public class ApiImplementationException extends ApiException
{
    public ApiImplementationException(SQLException sqlExceptionRef)
    {
        super(sqlExceptionRef);
    }
}
