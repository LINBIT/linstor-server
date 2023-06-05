package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.linstor.dbdrivers.DatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SatelliteDbDriver implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    static
    {
        try
        {
            DFLT_SERVICE_INSTANCE_NAME = new ServiceName("EmptyDatabaseService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The builtin default service instance name is not a valid ServiceName",
                nameExc
            );
        }
    }

    @Inject
    public SatelliteDbDriver()
    {
    }

    @Override
    public void loadSecurityObjects() throws DatabaseException
    {
        // no-op
    }

    @Override
    public void loadCoreObjects()
    {
        // no-op
    }

    @Override
    public ServiceName getDefaultServiceInstanceName()
    {
        return DFLT_SERVICE_INSTANCE_NAME;
    }
}
