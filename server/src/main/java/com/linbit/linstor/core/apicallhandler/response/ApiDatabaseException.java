package com.linbit.linstor.core.apicallhandler.response;

import com.linbit.linstor.dbdrivers.DatabaseException;

public class ApiDatabaseException extends ApiException
{
    public ApiDatabaseException(DatabaseException sqlExceptionRef)
    {
        super(sqlExceptionRef);
    }
}
