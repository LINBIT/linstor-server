package com.linbit.linstor.core;

import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.crypto.LengthPadding;
import com.linbit.crypto.LengthPaddingImpl;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.transaction.TransactionMap;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

public class CoreModule extends AbstractModule
{
    public static final String MODULE_NAME = "ModuleName";

    public static final String RECONFIGURATION_LOCK = "reconfigurationLock";
    public static final String CTRL_CONF_LOCK = "ctrlConfLock";
    public static final String NODES_MAP_LOCK = "nodesMapLock";
    public static final String RSC_DFN_MAP_LOCK = "rscDfnMapLock";
    public static final String STOR_POOL_DFN_MAP_LOCK = "storPoolDfnMapLock";
    public static final String FREE_SPACE_MGR_MAP_LOCK = "freeSpaceMgrMapLock";
    public static final String KVS_MAP_LOCK = "keyValueStoreMapLock";
    public static final String RSC_GROUP_MAP_LOCK = "rscGrpMapLock";

    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    // Path to the DRBD configuration files; this should be replaced by some meaningful constant or possibly
    // a value configurable in the cluster configuration
    public static final String CONFIG_PATH = "/var/lib/linstor.d";
    public static final String BACKUP_PATH = "/var/lib/linstor.d/.backup";

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
        bind(ResourceDefinitionMapExtName.class).to(ResourceDefinitionMapExtNameImpl.class);
        bind(StorPoolDefinitionMap.class).to(StorPoolDefinitionMapImpl.class);
        bind(KeyValueStoreMap.class).to(KeyValueStoreMapImpl.class);
        bind(ResourceGroupMap.class).to(ResourceGroupMapImpl.class);

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
        bind(ReadWriteLock.class).annotatedWith(Names.named(CTRL_CONF_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(KVS_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(ReadWriteLock.class).annotatedWith(Names.named(RSC_GROUP_MAP_LOCK))
            .toInstance(new ReentrantReadWriteLock(true));
        bind(LengthPadding.class).to(LengthPaddingImpl.class);
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

    public interface ResourceDefinitionMapExtName extends Map<byte[], ResourceDefinition>
    {
    }

    public interface StorPoolDefinitionMap extends Map<StorPoolName, StorPoolDefinition>
    {
    }

    public interface KeyValueStoreMap extends Map<KeyValueStoreName, KeyValueStore>
    {
    }

    public interface ResourceGroupMap extends Map<ResourceGroupName, ResourceGroup>
    {
    }

    public interface PeerMap extends Map<String, Peer>
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
    public static class ResourceDefinitionMapExtNameImpl
        extends TransactionMap<byte[], ResourceDefinition> implements ResourceDefinitionMapExtName
    {
        private static final int UNSIGNED_BYTE_MAX = 0xFF;

        private static final Comparator<? super byte[]> EXT_NAME_COMP = (final byte[] data1st, final byte[] data2nd) ->
        {
            int result = 0;

            final int cmpLength = Math.min(data1st.length, data2nd.length);
            int idx = 0;
            while (idx < cmpLength && result == 0)
            {
                final int unsigned1st = (data1st[idx]) & UNSIGNED_BYTE_MAX;
                final int unsigned2nd = (data2nd[idx]) & UNSIGNED_BYTE_MAX;

                if (unsigned1st < unsigned2nd)
                {
                    result = -1;
                }
                else
                if (unsigned1st > unsigned2nd)
                {
                    result = 1;
                }

                ++idx;
            }

            if (result == 0)
            {
                if (data1st.length < data2nd.length)
                {
                    result = -1;
                }
                else
                if (data1st.length > data2nd.length)
                {
                    result = 1;
                }
            }

            return result;
        };

        @Inject
        public ResourceDefinitionMapExtNameImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(new TreeMap<byte[], ResourceDefinition>(EXT_NAME_COMP), null, transMgrProvider);
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
    public static class KeyValueStoreMapImpl
        extends TransactionMap<KeyValueStoreName, KeyValueStore> implements KeyValueStoreMap
    {
        @Inject
        public KeyValueStoreMapImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(new TreeMap<>(), null, transMgrProvider);
        }
    }

    @Singleton
    public static class ResourceGroupMapImpl
    extends TransactionMap<ResourceGroupName, ResourceGroup> implements ResourceGroupMap
    {
        @Inject
        public ResourceGroupMapImpl(Provider<TransactionMgr> transMgrProvider)
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
