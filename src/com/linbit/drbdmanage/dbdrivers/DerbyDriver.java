package com.linbit.drbdmanage.dbdrivers;

import java.util.Map;
import java.util.WeakHashMap;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ServiceName;
import com.linbit.drbdmanage.NetInterfaceDataDatabaseDriver;
import com.linbit.drbdmanage.NetInterfaceDataDerbyDriver;
import com.linbit.drbdmanage.NetInterfaceName;
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
import com.linbit.drbdmanage.VolumeData;
import com.linbit.drbdmanage.VolumeDataDefinitionDerbyDriver;
import com.linbit.drbdmanage.VolumeDataDerbyDriver;
import com.linbit.drbdmanage.VolumeNumber;
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
    private final AccessContext dbCtx;
    private final NodeDataDerbyDriver nodeDriver;
    private final VolumeDataDerbyDriver volumeDriver;

    private Map<String, PropsConDerbyDriver> propsDriverCache = new WeakHashMap<>();
    private Map<String, NodeDataDerbyDriver> nodeDriverCache = new WeakHashMap<>();
    private Map<Tuple<NodeName, ResourceName>, ResourceDataDerbyDriver> resDriverCache = new WeakHashMap<>();
    private Map<ResourceName, ResourceDefinitionDataDerbyDriver> resDefDriverCache = new WeakHashMap<>();
    private Map<VolumeData, VolumeDataDerbyDriver> volDriverCache = new WeakHashMap<>();
    private Map<Tuple<ResourceDefinition, VolumeNumber>, VolumeDataDefinitionDerbyDriver> volDefDriverCache = new WeakHashMap<>();
    private Map<StorPoolName, StorPoolDefinitionDataDerbyDriver> storPoolDfnDriverCache = new WeakHashMap<>();
    private Map<Tuple<Node, StorPoolDefinition>, StorPoolDataDerbyDriver> storPoolDriverCache = new WeakHashMap<>();
    private Map<Tuple<Node, NetInterfaceName>, NetInterfaceDataDerbyDriver> netInterfaceDriverCache = new WeakHashMap<>();


    public DerbyDriver(ErrorReporter errorReporter, AccessContext privCtx)
    {
        this.errorReporter = errorReporter;
        this.dbCtx = privCtx;
        nodeDriver = new NodeDataDerbyDriver(privCtx);
        volumeDriver = new VolumeDataDerbyDriver(privCtx);
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
        PropsConDerbyDriver driver = propsDriverCache.get(instanceName);
        if (driver == null)
        {
            driver = new PropsConDerbyDriver(instanceName);
            propsDriverCache.put(instanceName, driver);
        }
        return driver;
    }


    @Override
    public NodeDataDatabaseDriver getNodeDatabaseDriver()
    {
        return nodeDriver;
    }

    @Override
    public ResourceDataDatabaseDriver getResourceDataDatabaseDriver(NodeName nodeName, ResourceName resName)
    {
        Tuple<NodeName, ResourceName> key = new Tuple<NodeName, ResourceName>(nodeName, resName);
        ResourceDataDerbyDriver driver = resDriverCache.get(key);
        if (driver == null)
        {
            driver = new ResourceDataDerbyDriver(dbCtx, nodeName, resName);
            resDriverCache.put(key, driver);
        }
        return driver;
    }

    @Override
    public ResourceDefinitionDataDatabaseDriver getResourceDefinitionDataDatabaseDriver(ResourceName resName)
    {
        ResourceDefinitionDataDerbyDriver driver= resDefDriverCache.get(resName);
        if (driver == null)
        {
            driver = new ResourceDefinitionDataDerbyDriver(resName);
            resDefDriverCache.put(resName, driver);
        }
        return driver;
    }

    @Override
    public VolumeDataDatabaseDriver getVolumeDataDatabaseDriver()
    {
        return volumeDriver;
    }

    @Override
    public VolumeDefinitionDataDatabaseDriver getVolumeDefinitionDataDatabaseDriver(ResourceDefinition resDfn, VolumeNumber volNr)
    {
        Tuple<ResourceDefinition, VolumeNumber> key = new Tuple<ResourceDefinition, VolumeNumber>(resDfn, volNr);
        VolumeDataDefinitionDerbyDriver driver = volDefDriverCache.get(key);
        if (driver == null)
        {
            driver = new VolumeDataDefinitionDerbyDriver(dbCtx, resDfn, volNr);
            volDefDriverCache.put(key, driver);
        }
        return driver;
    }

    @Override
    public StorPoolDefinitionDataDatabaseDriver getStorPoolDefinitionDataDatabaseDriver(StorPoolName name)
    {
        StorPoolDefinitionDataDerbyDriver driver = storPoolDfnDriverCache.get(name);
        if (driver == null)
        {
            driver = new StorPoolDefinitionDataDerbyDriver(name);
            storPoolDfnDriverCache.put(name, driver);
        }
        return driver;
    }

    @Override
    public StorPoolDataDatabaseDriver getStorPoolDataDatabaseDriver(Node nodeRef, StorPoolDefinition storPoolDfnRef)
    {
        Tuple<Node, StorPoolDefinition> key = new Tuple<Node, StorPoolDefinition>(nodeRef, storPoolDfnRef);
        StorPoolDataDerbyDriver driver = storPoolDriverCache.get(key);
        if (driver == null)
        {
            driver = new StorPoolDataDerbyDriver(nodeRef, storPoolDfnRef);
            storPoolDriverCache.put(key, driver);
        }
        return driver;
    }

    @Override
    public NetInterfaceDataDatabaseDriver getNetInterfaceDataDatabaseDriver(Node nodeRef, NetInterfaceName netName)
    {
        Tuple<Node, NetInterfaceName> key = new Tuple<Node, NetInterfaceName>(nodeRef, netName);
        NetInterfaceDataDerbyDriver driver = netInterfaceDriverCache.get(key);
        if (driver == null)
        {
            driver = new NetInterfaceDataDerbyDriver(dbCtx, nodeRef, netName);
            netInterfaceDriverCache.put(key, driver);
        }
        return driver;
    }



    private static class Tuple<A, B>
    {
        public final A a;
        public final B b;

        public Tuple(A a, B b)
        {
            this.a = a;
            this.b = b;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((a == null) ? 0 : a.hashCode());
            result = prime * result + ((b == null) ? 0 : b.hashCode());
            return result;
        }

        @SuppressWarnings("rawtypes")
        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Tuple other = (Tuple) obj;
            if (a == null)
            {
                if (other.a != null)
                    return false;
            }
            else
                if (!a.equals(other.a))
                    return false;
            if (b == null)
            {
                if (other.b != null)
                    return false;
            }
            else
                if (!b.equals(other.b))
                    return false;
            return true;
        }
    }

}
