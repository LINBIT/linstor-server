package com.linbit.drbdmanage.dbdrivers;

import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.ConnectionDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.NetInterfaceDataDerbyDriver;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeDataDerbyDriver;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.ResourceDataDerbyDriver;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPoolDataDerbyDriver;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.VolumeDataDerbyDriver;
import com.linbit.drbdmanage.VolumeDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ConnectionDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.drbdmanage.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
import com.linbit.drbdmanage.propscon.PropsConDerbyDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;

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

    private final NodeDataDerbyDriver nodeDriver;
    private final PropsConDerbyDriver propsDriver;
    private final ResourceDataDerbyDriver resourceDriver;
    private final ResourceDefinitionDataDerbyDriver resesourceDefinitionDriver;
    private final VolumeDefinitionDataDerbyDriver volumeDefinitionDriver;
    private final VolumeDataDerbyDriver volumeDriver;
    private final StorPoolDefinitionDataDerbyDriver storPoolDefinitionDriver;
    private final StorPoolDataDerbyDriver storPoolDriver;
    private final NetInterfaceDataDerbyDriver netInterfaceDriver;
    private final ConnectionDefinitionDataDerbyDriver connectionDefinitionDriver;

    public DerbyDriver(
        AccessContext privCtx,
        ErrorReporter errorReporterRef,
        Map<NodeName, Node> nodesMap,
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMap
    )
    {
        // DO NOT CHANGE THE INIT-ORDER!
        // some of the drivers depend on other drivers

        // no dependencies
        netInterfaceDriver = new NetInterfaceDataDerbyDriver(privCtx, errorReporterRef);
        connectionDefinitionDriver = new ConnectionDefinitionDataDerbyDriver(privCtx, errorReporterRef);
        propsDriver = new PropsConDerbyDriver(errorReporterRef);
        volumeDefinitionDriver = new VolumeDefinitionDataDerbyDriver(privCtx, errorReporterRef);
        volumeDriver = new VolumeDataDerbyDriver(privCtx, errorReporterRef);
        storPoolDefinitionDriver = new StorPoolDefinitionDataDerbyDriver(errorReporterRef, storPoolDfnMap);
        storPoolDriver = new StorPoolDataDerbyDriver(privCtx, errorReporterRef);

        // depends on volume
        resourceDriver = new ResourceDataDerbyDriver(
            privCtx,
            errorReporterRef,
            volumeDriver
        );

        // depends on netInterface, res and storPool
        nodeDriver = new NodeDataDerbyDriver(
            privCtx,
            errorReporterRef,
            nodesMap,
            netInterfaceDriver,
            resourceDriver,
            storPoolDriver
        );

        // depends on conDfn, resData, volDfn
        resesourceDefinitionDriver = new ResourceDefinitionDataDerbyDriver(
            privCtx,
            errorReporterRef,
            rscDfnMap,
            connectionDefinitionDriver,
            resourceDriver,
            volumeDefinitionDriver
        );
        errorReporterRef.logError("Error");
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
    public PropsConDatabaseDriver getPropsDatabaseDriver()
    {
        return propsDriver;
    }


    @Override
    public NodeDataDatabaseDriver getNodeDatabaseDriver()
    {
        return nodeDriver;
    }

    @Override
    public ResourceDataDatabaseDriver getResourceDataDatabaseDriver()
    {
        return resourceDriver;
    }

    @Override
    public ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver()
    {
        return resesourceDefinitionDriver;
    }

    @Override
    public VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver()
    {
        return volumeDefinitionDriver;
    }

    @Override
    public VolumeDataDatabaseDriver getVolumeDataDatabaseDriver()
    {
        return volumeDriver;
    }

    @Override
    public StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver()
    {
        return storPoolDefinitionDriver;
    }

    @Override
    public StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver()
    {
        return storPoolDriver;
    }

    @Override
    public NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver()
    {
        return netInterfaceDriver;
    }

    @Override
    public ConnectionDefinitionDataDatabaseDriver getConnectionDefinitionDatabaseDriver()
    {
        return connectionDefinitionDriver;
    }

    public static void handleAccessDeniedException(AccessDeniedException accDeniedExc)
        throws ImplementationError
    {
        throw new ImplementationError(
            "Database's access context has insufficient permissions",
            accDeniedExc
        );
    }
}
