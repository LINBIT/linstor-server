package com.linbit.drbdmanage.dbdrivers;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.Volume;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.dbdrivers.derby.NodeDerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.PropsConDerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.ResourceDefinitionDerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.ResourceDerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.VolumeDefinitionDerbyDriver;
import com.linbit.drbdmanage.dbdrivers.derby.VolumeDerbyDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DerbyDriver implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    public static String DB_CONNECTION_URL = "jdbc:derby:directory:database";

    static
    {
        try
        {
            DFLT_SERVICE_INSTANCE_NAME = new ServiceName("DerbyDatabaseService");
        }
        catch (InvalidNameException nameExc)
        {
            throw new ImplementationError(
                "The builtin default service instance name is not a valid ServiceName",
                nameExc
            );
        }
    }

    private ErrorReporter errorReporter;

    public DerbyDriver(ErrorReporter errorReporter)
    {
        this.errorReporter = errorReporter;
    }

    @Override
    public ServiceName getDefaultServiceInstanceName()
    {
        return DFLT_SERVICE_INSTANCE_NAME;
    }

    @Override
    public String getDefaultConnectionUrl()
    {
        return DB_CONNECTION_URL;
    }

    @Override
    public PropsConDatabaseDriver getPropsDatabaseDriver(Connection con, String instanceName) throws SQLException
    {
        return new PropsConDerbyDriver(instanceName, con);
    }

    @Override
    public NodeDatabaseDriver getNodeDatabaseDriver(String nodeName)
    {
        return new NodeDerbyDriver(nodeName);
    }

    @Override
    public ResourceDatabaseDriver getResourceDatabaseDriver(Resource res)
    {
        return new ResourceDerbyDriver(res);
    }

    @Override
    public ResourceDefinitionDatabaseDriver getResourceDefinitionDatabaseDriver(ResourceDefinition resDfn)
    {
        return new ResourceDefinitionDerbyDriver(resDfn);
    }

    @Override
    public VolumeDatabaseDriver getVolumeDatabaseDriver(Volume volume)
    {
        return new VolumeDerbyDriver(volume);
    }

    @Override
    public VolumeDefinitionDatabaseDriver getVolumeDefinitionDatabaseDriver(VolumeDefinition volumeDefinition)
    {
        return new VolumeDefinitionDerbyDriver(volumeDefinition);
    }
}
