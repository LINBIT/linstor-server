package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolDefinition;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.netcom.Peer;

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

    @Override
    protected void configure()
    {
        bind(new TypeLiteral<Map<ServiceName, SystemService>>() {})
            .toInstance(new TreeMap<ServiceName, SystemService>());

        bind(NodesMap.class).toInstance(new NodesMapImpl());
        bind(new TypeLiteral<Map<NodeName, Node>>() {}).to(NodesMap.class);

        bind(ResourceDefinitionMap.class).toInstance(new ResourceDefinitionMapImpl());
        bind(new TypeLiteral<Map<ResourceName, ResourceDefinition>>() {}).to(ResourceDefinitionMap.class);

        bind(StorPoolDefinitionMap.class).toInstance(new StorPoolDefinitionMapImpl());
        bind(new TypeLiteral<Map<StorPoolName, StorPoolDefinition>>() {}).to(StorPoolDefinitionMap.class);

        bind(PeerMap.class).toInstance(new PeerMapImpl());
        bind(new TypeLiteral<Map<String, Peer>>() {}).to(PeerMap.class);

        bind(ReadWriteLock.class).annotatedWith(Names.named(RECONFIGURATION_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(NODES_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(RSC_DFN_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(STOR_POOL_DFN_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
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

    public static class NodesMapImpl
        extends TreeMap<NodeName, Node> implements NodesMap
    {
    }

    public static class ResourceDefinitionMapImpl
        extends TreeMap<ResourceName, ResourceDefinition> implements ResourceDefinitionMap
    {
    }

    public static class StorPoolDefinitionMapImpl
        extends TreeMap<StorPoolName, StorPoolDefinition> implements StorPoolDefinitionMap
    {
    }

    public static class PeerMapImpl
        extends TreeMap<String, Peer> implements PeerMap
    {
    }
}
