package com.linbit.drbdmanage.dbdrivers;

import java.util.HashMap;
import java.util.Map;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.ConnectionDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.NetInterfaceDataDerbyDriver;
import com.linbit.drbdmanage.NetInterfaceName;
import com.linbit.drbdmanage.Node;
import com.linbit.drbdmanage.NodeDataDerbyDriver;
import com.linbit.drbdmanage.NodeName;
import com.linbit.drbdmanage.Resource;
import com.linbit.drbdmanage.ResourceDataDerbyDriver;
import com.linbit.drbdmanage.ResourceDefinition;
import com.linbit.drbdmanage.ResourceDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.ResourceName;
import com.linbit.drbdmanage.StorPoolDataDerbyDriver;
import com.linbit.drbdmanage.StorPoolDefinition;
import com.linbit.drbdmanage.StorPoolDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.StorPoolName;
import com.linbit.drbdmanage.VolumeDataDerbyDriver;
import com.linbit.drbdmanage.VolumeDefinition;
import com.linbit.drbdmanage.VolumeDefinitionDataDerbyDriver;
import com.linbit.drbdmanage.VolumeNumber;
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
import com.linbit.drbdmanage.propscon.PropsConDerbyDriver;
import com.linbit.drbdmanage.security.AccessContext;

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

    private final AccessContext dbCtx;

    private final Map<PrimaryKey, NodeDataDerbyDriver> nodeDriverCache = new HashMap<>();
    private final Map<PrimaryKey, PropsConDerbyDriver> propsDriverCache = new HashMap<>();
    private final Map<PrimaryKey, ResourceDataDerbyDriver> resDriverCache = new HashMap<>();
    private final Map<PrimaryKey, ResourceDefinitionDataDerbyDriver> resDefDriverCache = new HashMap<>();
    private final Map<PrimaryKey, VolumeDefinitionDataDerbyDriver> volDefDriverCache = new HashMap<>();
    private final Map<PrimaryKey, VolumeDataDerbyDriver> volDriverCache = new HashMap<>();
    private final Map<PrimaryKey, StorPoolDefinitionDataDerbyDriver> storPoolDfnDriverCache = new HashMap<>();
    private final Map<PrimaryKey, StorPoolDataDerbyDriver> storPoolDriverCache = new HashMap<>();
    private final Map<PrimaryKey, NetInterfaceDataDerbyDriver> netInterfaceDriverCache = new HashMap<>();
    private final Map<PrimaryKey, ConnectionDefinitionDataDerbyDriver> conDfnDriverCache = new HashMap<>();


    public DerbyDriver(AccessContext privCtx)
    {
        this.dbCtx = privCtx;
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
    public PropsConDatabaseDriver getPropsDatabaseDriver(String instanceName)
    {
        PrimaryKey pk = new PrimaryKey(instanceName);
        PropsConDerbyDriver driver = propsDriverCache.get(pk);
        if (driver == null)
        {
            driver = new PropsConDerbyDriver(instanceName);
            propsDriverCache.put(pk, driver);
        }
        return driver;
    }


    @Override
    public NodeDataDatabaseDriver getNodeDatabaseDriver(NodeName nodeName)
    {
        PrimaryKey pk = new PrimaryKey(nodeName);
        NodeDataDerbyDriver driver = nodeDriverCache.get(pk);
        if (driver == null)
        {
            driver = new NodeDataDerbyDriver(dbCtx, nodeName);
            nodeDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public ResourceDataDatabaseDriver getResourceDataDatabaseDriver(NodeName nodeName, ResourceName resName)
    {
        PrimaryKey pk = new PrimaryKey(resName, nodeName);
        ResourceDataDerbyDriver driver = resDriverCache.get(pk);
        if (driver == null)
        {
            driver = new ResourceDataDerbyDriver(dbCtx, nodeName, resName);
            resDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver(ResourceName resName)
    {
        PrimaryKey pk = new PrimaryKey(resName);
        ResourceDefinitionDataDerbyDriver driver= resDefDriverCache.get(pk);
        if (driver == null)
        {
            driver = new ResourceDefinitionDataDerbyDriver(dbCtx, resName);
            resDefDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver(ResourceDefinition resDfn, VolumeNumber volNr)
    {
        PrimaryKey pk = new PrimaryKey(resDfn, volNr);
        VolumeDefinitionDataDerbyDriver driver = volDefDriverCache.get(pk);
        if (driver == null)
        {
            driver = new VolumeDefinitionDataDerbyDriver(dbCtx, resDfn, volNr);
            volDefDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public VolumeDataDatabaseDriver getVolumeDataDatabaseDriver(Resource resRef, VolumeDefinition volDfnRef)
    {
        PrimaryKey pk = new PrimaryKey(resRef, volDfnRef);
        VolumeDataDerbyDriver driver= volDriverCache.get(pk);
        if (driver == null)
        {
            driver = new VolumeDataDerbyDriver(dbCtx, resRef, volDfnRef);
            volDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver(StorPoolName name)
    {
        PrimaryKey pk = new PrimaryKey(name);
        StorPoolDefinitionDataDerbyDriver driver = storPoolDfnDriverCache.get(pk);
        if (driver == null)
        {
            driver = new StorPoolDefinitionDataDerbyDriver(name);
            storPoolDfnDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver(Node nodeRef, StorPoolDefinition storPoolDfnRef)
    {
        PrimaryKey pk = new PrimaryKey(nodeRef, storPoolDfnRef);
        StorPoolDataDerbyDriver driver = storPoolDriverCache.get(pk);
        if (driver == null)
        {
            driver = new StorPoolDataDerbyDriver(nodeRef, storPoolDfnRef);
            storPoolDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver(Node nodeRef, NetInterfaceName netName)
    {
        PrimaryKey pk = new PrimaryKey(nodeRef, netName);
        NetInterfaceDataDerbyDriver driver = netInterfaceDriverCache.get(pk);
        if (driver == null)
        {
            driver = new NetInterfaceDataDerbyDriver(dbCtx, nodeRef, netName);
            netInterfaceDriverCache.put(pk, driver);
        }
        return driver;
    }

    @Override
    public ConnectionDefinitionDataDatabaseDriver getConnectionDefinitionDatabaseDriver(
        ResourceName resName,
        NodeName sourceNodeName,
        NodeName targetNodeName
    )
    {
        PrimaryKey pk = new PrimaryKey(resName, sourceNodeName, targetNodeName);
        ConnectionDefinitionDataDerbyDriver driver = conDfnDriverCache.get(pk);
        if (driver == null)
        {
            driver = new ConnectionDefinitionDataDerbyDriver(dbCtx, resName, sourceNodeName, targetNodeName);
            conDfnDriverCache.put(pk, driver);
        }
        return driver;
    }
}
