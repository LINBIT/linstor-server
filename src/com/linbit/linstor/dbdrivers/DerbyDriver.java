package com.linbit.linstor.dbdrivers;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsConDerbyDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

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

    private Map<NodeName, Node> nodesMap;
    private Map<ResourceName, ResourceDefinition> rscDfnMap;
    private Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;

    public DerbyDriver(
        AccessContext privCtx,
        ErrorReporter errorReporterRef,
        Map<NodeName, Node> nodesMap,
        Map<ResourceName, ResourceDefinition> rscDfnMap,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMap
    )
    {
        this.nodesMap = nodesMap;
        this.rscDfnMap = rscDfnMap;
        this.storPoolDfnMap = storPoolDfnMap;
        propsDriver = new PropsConDerbyDriver(errorReporterRef);
        nodeDriver = new NodeDataDerbyDriver(privCtx, errorReporterRef, nodesMap);
        resesourceDefinitionDriver = new ResourceDefinitionDataDerbyDriver(
            privCtx,
            errorReporterRef,
            rscDfnMap
        );
        resourceDriver = new ResourceDataDerbyDriver(privCtx, errorReporterRef);
        volumeDefinitionDriver = new VolumeDefinitionDataDerbyDriver(privCtx, errorReporterRef);
        volumeDriver = new VolumeDataDerbyDriver(privCtx, errorReporterRef);
        storPoolDefinitionDriver = new StorPoolDefinitionDataDerbyDriver(errorReporterRef, storPoolDfnMap);
        storPoolDriver = new StorPoolDataDerbyDriver(privCtx, errorReporterRef);
        netInterfaceDriver = new NetInterfaceDataDerbyDriver(privCtx, errorReporterRef);
        satelliteConnectionDriver = new SatelliteConnectionDataDerbyDriver(privCtx, errorReporterRef);
        nodeConnectionDriver = new NodeConnectionDataDerbyDriver(
            privCtx,
            errorReporterRef
        );
        resourceConnectionDriver = new ResourceConnectionDataDerbyDriver(
            privCtx,
            errorReporterRef
        );
        volumeConnectionDriver = new VolumeConnectionDataDerbyDriver(
            privCtx,
            errorReporterRef
        );

        // propsDriver.initialize();
        nodeDriver.initialize(
            netInterfaceDriver,
            satelliteConnectionDriver,
            resourceDriver,
            storPoolDriver,
            nodeConnectionDriver
        );
        satelliteConnectionDriver.initialize(netInterfaceDriver);
        resesourceDefinitionDriver.initialize(resourceDriver, volumeDefinitionDriver);
        resourceDriver.initialize(resourceConnectionDriver, volumeDriver);
        // volumeDefinitionDriver.initialize();
        volumeDriver.initialize(nodeDriver, resourceDriver, volumeConnectionDriver);
        // storPoolDefinitionDriver.initialize();
        storPoolDriver.initialize(volumeDriver);
        // netInterfaceDriver.initialize();
        nodeConnectionDriver.initialize(nodeDriver);
        resourceConnectionDriver.initialize(nodeDriver, resourceDriver);
        volumeConnectionDriver.initialize(
            nodeDriver,
            resesourceDefinitionDriver,
            resourceDriver,
            volumeDefinitionDriver,
            volumeDriver
        );
    }

    @Override
    public void loadAll(TransactionMgr transMgr) throws SQLException
    {
        List<NodeData> nodeList = nodeDriver.loadAll(transMgr);
        List<ResourceDefinitionData> rscDfnList = resesourceDefinitionDriver.loadAll(transMgr);
        List<StorPoolDefinitionData> storPoolDfnList = storPoolDefinitionDriver.loadAll(transMgr);

        nodeDriver.clearCache();
        resesourceDefinitionDriver.clearCache();
        resourceDriver.clearCache();
        volumeDriver.clearCache();
        storPoolDriver.clearCache();

        for (Node curNode : nodeList)
        {
            nodesMap.put(curNode.getName(), curNode);
        }
        for (ResourceDefinition curRscDfn : rscDfnList)
        {
            rscDfnMap.put(curRscDfn.getName(), curRscDfn);
        }
        for (StorPoolDefinition curStorPoolDfn : storPoolDfnList)
        {
            storPoolDfnMap.put(curStorPoolDfn.getName(), curStorPoolDfn);
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
