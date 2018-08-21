package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.FreeSpaceMgr;
import com.linbit.linstor.FreeSpaceMgrName;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CoreModule extends AbstractModule
{
    public static final String MODULE_NAME = "ModuleName";

    public static final String RECONFIGURATION_LOCK = "reconfigurationLock";
    public static final String NODES_MAP_LOCK = "nodesMapLock";
    public static final String RSC_DFN_MAP_LOCK = "rscDfnMapLock";
    public static final String STOR_POOL_DFN_MAP_LOCK = "storPoolDfnMapLock";
    public static final String FREE_SPACE_MGR_MAP_LOCK = "freeSpaceMgrMapLock";

    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    @Override
    protected void configure()
    {
        bind(new TypeLiteral<Map<ServiceName, SystemService>>()
            {
            }
            )
            .toInstance(new TreeMap<>());

        bind(NodesMap.class).to(NodesMapImpl.class);
        bind(ResourceDefinitionMap.class).to(ResourceDefinitionMapImpl.class);
        bind(StorPoolDefinitionMap.class).to(StorPoolDefinitionMapImpl.class);
        bind(FreeSpaceMgrMap.class).to(FreeSpaceMgrMapImpl.class);

        bind(PeerMap.class).toInstance(new PeerMapImpl());

        bind(ReadWriteLock.class).annotatedWith(Names.named(RECONFIGURATION_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(NODES_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(RSC_DFN_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(STOR_POOL_DFN_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(FREE_SPACE_MGR_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
    }

    @Provides
    @Singleton
    @Named(LinStor.SATELLITE_PROPS)
    public Props createSatellitePropsContainer(PropsContainerFactory propsContainerFactory)
    {
        return propsContainerFactory.create(DB_SATELLITE_PROPSCON_INSTANCE_NAME);
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

    public interface PeerMap extends Map<String, Peer>
    {
    }

    public interface FreeSpaceMgrMap extends Map<FreeSpaceMgrName, FreeSpaceMgr>
    {
    }

    @Singleton
    public static class NodesMapImpl
        extends TransactionMap<NodeName, Node> implements NodesMap
    {
        @Inject
        public NodesMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(new TreeMap<>(), null, transMgrProvider);
        }
    }

    @Singleton
    public static class ResourceDefinitionMapImpl
        extends TransactionMap<ResourceName, ResourceDefinition> implements ResourceDefinitionMap
    {
        @Inject
        public ResourceDefinitionMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(new TreeMap<>(), null, transMgrProvider);
        }
    }

    @Singleton
    public static class StorPoolDefinitionMapImpl
        extends TransactionMap<StorPoolName, StorPoolDefinition> implements StorPoolDefinitionMap
    {
        @Inject
        public StorPoolDefinitionMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(new TreeMap<>(), null, transMgrProvider);
        }
    }

    @Singleton
    public static class FreeSpaceMgrMapImpl
        extends TransactionMap<FreeSpaceMgrName, FreeSpaceMgr> implements FreeSpaceMgrMap
    {
        @Inject
        public FreeSpaceMgrMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(new TreeMap<>(), null, transMgrProvider);
        }
    }


    @Singleton
    public static class PeerMapImpl
        extends TreeMap<String, Peer> implements PeerMap
    // intentionally not a TransactionMap, as the Peer interface would have to extend
    // TransactionObject, which makes no sense as something like a "socket.close()" cannot
    // be rolled back
    {
        @Inject
        public PeerMapImpl()
        {
        }
    }
}
