package com.linbit.linstor.core;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.linbit.ServiceName;
import com.linbit.SystemService;
import com.linbit.linstor.KeyValueStore;
import com.linbit.linstor.KeyValueStoreName;
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
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private static final String DB_SATELLITE_PROPSCON_INSTANCE_NAME = "STLTCFG";

    // Path to the DRBD configuration files; this should be replaced by some meaningful constant or possibly
    // a value configurable in the cluster configuration
    public static final String CONFIG_PATH = "/var/lib/linstor.d";


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
        private static final int NEW_MAX_BYTE_VALUE = 256;
        private static final Comparator<? super byte[]> EXT_NAME_COMP = (byte[] extName1, byte[] extName2) ->
        {
            int ret;
            int index = 0;
            while (true)
            {
                index++;
                if (index > extName1.length - 1 && !(index > extName2.length - 1))
                {
                    ret = -1;
                    break;
                }
                else if (index > extName1.length - 1 && index > extName2.length - 1)
                {
                    ret = 0;
                    break;
                }
                else if (index > extName2.length - 1 && !(index > extName1.length - 1))
                {
                    ret = 1;
                    break;
                }

                int intValue1 = byteToUnsignedInt(extName1[index]);
                int intValue2 = byteToUnsignedInt(extName2[index]);
                if (intValue1 < intValue2)
                {
                    ret = -1;
                    break;
                }
                if (intValue1 > intValue2)
                {
                    ret = 1;
                    break;
                }
            }
            return ret;
        };

        @Inject
        public ResourceDefinitionMapExtNameImpl(Provider<TransactionMgr> transMgrProvider)
        {
            super(new TreeMap<byte[], ResourceDefinition>(EXT_NAME_COMP), null, transMgrProvider);
        }

        private static int byteToUnsignedInt(byte byteValue)
        {
            int intValue = byteValue;
            if (intValue < 0)
            {
                intValue += NEW_MAX_BYTE_VALUE;
            }
            return intValue;
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
