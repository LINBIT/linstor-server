package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.TransactionMgr;
import com.linbit.linstor.NetInterfaceDataDerbyDriver;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeConnectionDataDerbyDriver;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeDataDerbyDriver;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceConnectionDataDerbyDriver;
import com.linbit.linstor.ResourceDataDerbyDriver;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceDefinitionDataDerbyDriver;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SatelliteConnectionDataDerbyDriver;
import com.linbit.linstor.StorPoolDataDerbyDriver;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolDefinitionDataDerbyDriver;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.VolumeConnectionDataDerbyDriver;
import com.linbit.linstor.VolumeDataDerbyDriver;
import com.linbit.linstor.VolumeDefinitionDataDerbyDriver;
import com.linbit.linstor.dbdrivers.interfaces.NetInterfaceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.NodeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.PropsConDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.ResourceDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeConnectionDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDataDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.VolumeDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.PropsConDerbyDriver;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
@Singleton
public class DerbyDriver implements DatabaseDriver
{
    public static final ServiceName DFLT_SERVICE_INSTANCE_NAME;

    public static final String DEFAULT_DB_CONNECTION_URL = "jdbc:derby:directory:database";

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

    private final PropsConDerbyDriver propsDriver;
    private final NodeDataDerbyDriver nodeDriver;
    private final ResourceDefinitionDataDerbyDriver resesourceDefinitionDriver;
    private final ResourceDataDerbyDriver resourceDriver;
    private final VolumeDefinitionDataDerbyDriver volumeDefinitionDriver;
    private final VolumeDataDerbyDriver volumeDriver;
    private final StorPoolDefinitionDataDerbyDriver storPoolDefinitionDriver;
    private final StorPoolDataDerbyDriver storPoolDriver;
    private final NetInterfaceDataDerbyDriver netInterfaceDriver;
    private final SatelliteConnectionDataDerbyDriver satelliteConnectionDriver;
    private final NodeConnectionDataDerbyDriver nodeConnectionDriver;
    private final ResourceConnectionDataDerbyDriver resourceConnectionDriver;
    private final VolumeConnectionDataDerbyDriver volumeConnectionDriver;

    private final Map<NodeName, Node> nodesMap;
    private final Map<ResourceName, ResourceDefinition> rscDfnMap;
    private final Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;

    @Inject
    public DerbyDriver(
        PropsConDerbyDriver propsDriverRef,
        NodeDataDerbyDriver nodeDriverRef,
        ResourceDefinitionDataDerbyDriver resesourceDefinitionDriverRef,
        ResourceDataDerbyDriver resourceDriverRef,
        VolumeDefinitionDataDerbyDriver volumeDefinitionDriverRef,
        VolumeDataDerbyDriver volumeDriverRef,
        StorPoolDefinitionDataDerbyDriver storPoolDefinitionDriverRef,
        StorPoolDataDerbyDriver storPoolDriverRef,
        NetInterfaceDataDerbyDriver netInterfaceDriverRef,
        SatelliteConnectionDataDerbyDriver satelliteConnectionDriverRef,
        NodeConnectionDataDerbyDriver nodeConnectionDriverRef,
        ResourceConnectionDataDerbyDriver resourceConnectionDriverRef,
        VolumeConnectionDataDerbyDriver volumeConnectionDriverRef,
        Map<NodeName, Node> nodesMapRef,
        Map<ResourceName, ResourceDefinition> rscDfnMapRef,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMapRef
    )
    {
        propsDriver = propsDriverRef;
        nodeDriver = nodeDriverRef;
        resesourceDefinitionDriver = resesourceDefinitionDriverRef;
        resourceDriver = resourceDriverRef;
        volumeDefinitionDriver = volumeDefinitionDriverRef;
        volumeDriver = volumeDriverRef;
        storPoolDefinitionDriver = storPoolDefinitionDriverRef;
        storPoolDriver = storPoolDriverRef;
        netInterfaceDriver = netInterfaceDriverRef;
        satelliteConnectionDriver = satelliteConnectionDriverRef;
        nodeConnectionDriver = nodeConnectionDriverRef;
        resourceConnectionDriver = resourceConnectionDriverRef;
        volumeConnectionDriver = volumeConnectionDriverRef;
        nodesMap = nodesMapRef;
        rscDfnMap = rscDfnMapRef;
        storPoolDfnMap = storPoolDfnMapRef;
    }

    @Override
    public void loadAll(TransactionMgr transMgr) throws SQLException
    {
        // order is somewhat important here, storage pool definitions should be loaded first
        // and added to the storPoolDfnMap, otherwise the later node loading will not correctly
        // link its storage pools with the definitions.

        List<StorPoolDefinitionData> storPoolDfnList = storPoolDefinitionDriver.loadAll(transMgr);
        for (StorPoolDefinition curStorPoolDfn : storPoolDfnList)
        {
            storPoolDfnMap.put(curStorPoolDfn.getName(), curStorPoolDfn);
        }

        List<NodeData> nodeList = nodeDriver.loadAll(transMgr);
        for (Node curNode : nodeList)
        {
            nodesMap.put(curNode.getName(), curNode);
        }

        List<ResourceDefinitionData> rscDfnList = resesourceDefinitionDriver.loadAll(transMgr);

        nodeDriver.clearCache();
        resesourceDefinitionDriver.clearCache();
        resourceDriver.clearCache();
        volumeDriver.clearCache();
        storPoolDriver.clearCache();

        for (ResourceDefinition curRscDfn : rscDfnList)
        {
            rscDfnMap.put(curRscDfn.getName(), curRscDfn);
        }
    }

    @Override
    public ServiceName getDefaultServiceInstanceName()
    {
        return DFLT_SERVICE_INSTANCE_NAME;
    }

    @Override
    public String getDefaultConnectionUrl()
    {
        return DEFAULT_DB_CONNECTION_URL;
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
    public SatelliteConnectionDataDerbyDriver getSatelliteConnectionDataDatabaseDriver()
    {
        return satelliteConnectionDriver;
    }

    @Override
    public NodeConnectionDataDatabaseDriver getNodeConnectionDataDatabaseDriver()
    {
        return nodeConnectionDriver;
    }

    @Override
    public ResourceConnectionDataDatabaseDriver getResourceConnectionDataDatabaseDriver()
    {
        return resourceConnectionDriver;
    }

    @Override
    public VolumeConnectionDataDatabaseDriver getVolumeConnectionDataDatabaseDriver()
    {
        return volumeConnectionDriver;
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
