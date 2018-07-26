package com.linbit.linstor.core.apicallhandler.response;

import java.sql.SQLException;

public class ApiSQLException extends ApiException
{
    public ApiSQLException(SQLException sqlExceptionRef)
    {
        super(sqlExceptionRef);
    }
}
