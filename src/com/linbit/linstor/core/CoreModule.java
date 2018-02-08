package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.TransactionMgr;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.dbcp.DbConnectionPool;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainer;

import javax.inject.Named;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CoreModule extends AbstractModule
{
    public static final String CONTROLLER_PROPS = "ControllerProps";

    public static final String RECONFIGURATION_LOCK = "reconfigurationLock";
    public static final String NODES_MAP_LOCK = "nodesMapLock";
    public static final String RSC_DFN_MAP_LOCK = "rscDfnMapLock";
    public static final String STOR_POOL_DFN_MAP_LOCK = "storPoolDfnMapLock";

    private static final String DB_CONTROLLER_PROPSCON_INSTANCE_NAME = "CTRLCFG";

    @Override
    protected void configure()
    {
        bind(NodesMap.class).toInstance(new NodesMapImpl());
        bind(new TypeLiteral<Map<NodeName, Node>>() {}).to(NodesMap.class);

        bind(ResourceDefinitionMap.class).toInstance(new ResourceDefinitionMapImpl());
        bind(new TypeLiteral<Map<ResourceName, ResourceDefinition>>() {}).to(ResourceDefinitionMap.class);

        bind(StorPoolDefinitionMap.class).toInstance(new StorPoolDefinitionMapImpl());
        bind(new TypeLiteral<Map<StorPoolName, StorPoolDefinition>>() {}).to(StorPoolDefinitionMap.class);

        bind(ReadWriteLock.class).annotatedWith(Names.named(RECONFIGURATION_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(NODES_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(RSC_DFN_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(STOR_POOL_DFN_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
    }

    @Provides
    @Singleton
    @Named(CONTROLLER_PROPS)
    private Props loadPropsContainer(DbConnectionPool dbConnPool)
        throws SQLException
    {
        Props propsContainer;
        TransactionMgr transMgr = null;
        try
        {
            transMgr = new TransactionMgr(dbConnPool);
            propsContainer = PropsContainer.getInstance(DB_CONTROLLER_PROPSCON_INSTANCE_NAME, transMgr);
            transMgr.commit();
        }
        finally
        {
            if (transMgr != null)
            {
                dbConnPool.returnConnection(transMgr);
            }
        }
        return propsContainer;
    }

    public interface NodesMap extends Map<NodeName, Node>
    {
    }

    public interface ResourceDefinitionMap extends Map<ResourceName, ResourceDefinition>
    {
    }

    public interface StorPoolDefinitionMap extends Map<StorPoolName, StorPoolDefinition>
    {
    }

    private static class NodesMapImpl
        extends HashMap<NodeName, Node> implements NodesMap
    {
    }

    private static class ResourceDefinitionMapImpl
        extends HashMap<ResourceName, ResourceDefinition> implements ResourceDefinitionMap
    {
    }

    private static class StorPoolDefinitionMapImpl
        extends HashMap<StorPoolName, StorPoolDefinition> implements StorPoolDefinitionMap
    {
    }
}
