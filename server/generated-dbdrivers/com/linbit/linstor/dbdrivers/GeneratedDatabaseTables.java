package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;

import java.sql.Types;

@SuppressWarnings("checkstyle:linelength")
public class GeneratedDatabaseTables
{
    private GeneratedDatabaseTables()
    {
    }

    // Schema name
    public static final String DATABASE_SCHEMA_NAME = "LINSTOR";

    public static class EbsRemotes implements DatabaseTable
    {
        private EbsRemotes()
        {
        }

        // Primary Key
        public static final ColumnImpl NAME = new ColumnImpl("NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl DSP_NAME = new ColumnImpl("DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl URL = new ColumnImpl("URL", Types.VARCHAR, false, false);
        public static final ColumnImpl REGION = new ColumnImpl("REGION", Types.VARCHAR, false, false);
        public static final ColumnImpl AVAILABILITY_ZONE = new ColumnImpl("AVAILABILITY_ZONE", Types.VARCHAR, false, false);
        public static final ColumnImpl ACCESS_KEY = new ColumnImpl("ACCESS_KEY", Types.BLOB, false, false);
        public static final ColumnImpl SECRET_KEY = new ColumnImpl("SECRET_KEY", Types.BLOB, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NAME,
            DSP_NAME,
            FLAGS,
            URL,
            REGION,
            AVAILABILITY_ZONE,
            ACCESS_KEY,
            SECRET_KEY
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "EBS_REMOTES";
        }

        @Override
        public String toString()
        {
            return "Table EBS_REMOTES";
        }
    }

    public static class Files implements DatabaseTable
    {
        private Files()
        {
        }

        // Primary Key
        public static final ColumnImpl PATH = new ColumnImpl("PATH", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl CONTENT = new ColumnImpl("CONTENT", Types.BLOB, false, false);
        public static final ColumnImpl CONTENT_CHECKSUM = new ColumnImpl("CONTENT_CHECKSUM", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            PATH,
            FLAGS,
            CONTENT,
            CONTENT_CHECKSUM
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "FILES";
        }

        @Override
        public String toString()
        {
            return "Table FILES";
        }
    }

    public static class KeyValueStore implements DatabaseTable
    {
        private KeyValueStore()
        {
        }

        // Primary Key
        public static final ColumnImpl KVS_NAME = new ColumnImpl("KVS_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl KVS_DSP_NAME = new ColumnImpl("KVS_DSP_NAME", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            KVS_NAME,
            KVS_DSP_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "KEY_VALUE_STORE";
        }

        @Override
        public String toString()
        {
            return "Table KEY_VALUE_STORE";
        }
    }

    public static class LayerBcacheVolumes implements DatabaseTable
    {
        private LayerBcacheVolumes()
        {
        }

        // Primary Keys
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
        public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, false, true);
        public static final ColumnImpl DEV_UUID = new ColumnImpl("DEV_UUID", Types.CHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            VLM_NR,
            NODE_NAME,
            POOL_NAME,
            DEV_UUID
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_BCACHE_VOLUMES";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_BCACHE_VOLUMES";
        }
    }

    public static class LayerCacheVolumes implements DatabaseTable
    {
        private LayerCacheVolumes()
        {
        }

        // Primary Keys
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
        public static final ColumnImpl POOL_NAME_CACHE = new ColumnImpl("POOL_NAME_CACHE", Types.VARCHAR, false, true);
        public static final ColumnImpl POOL_NAME_META = new ColumnImpl("POOL_NAME_META", Types.VARCHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            VLM_NR,
            NODE_NAME,
            POOL_NAME_CACHE,
            POOL_NAME_META
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_CACHE_VOLUMES";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_CACHE_VOLUMES";
        }
    }

    public static class LayerDrbdResources implements DatabaseTable
    {
        private LayerDrbdResources()
        {
        }

        // Primary Key
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);

        public static final ColumnImpl PEER_SLOTS = new ColumnImpl("PEER_SLOTS", Types.INTEGER, false, false);
        public static final ColumnImpl AL_STRIPES = new ColumnImpl("AL_STRIPES", Types.INTEGER, false, false);
        public static final ColumnImpl AL_STRIPE_SIZE = new ColumnImpl("AL_STRIPE_SIZE", Types.BIGINT, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl NODE_ID = new ColumnImpl("NODE_ID", Types.INTEGER, false, false);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            PEER_SLOTS,
            AL_STRIPES,
            AL_STRIPE_SIZE,
            FLAGS,
            NODE_ID
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_DRBD_RESOURCES";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_DRBD_RESOURCES";
        }
    }

    public static class LayerDrbdResourceDefinitions implements DatabaseTable
    {
        private LayerDrbdResourceDefinitions()
        {
        }

        // Primary Keys
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl RESOURCE_NAME_SUFFIX = new ColumnImpl("RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl PEER_SLOTS = new ColumnImpl("PEER_SLOTS", Types.INTEGER, false, false);
        public static final ColumnImpl AL_STRIPES = new ColumnImpl("AL_STRIPES", Types.INTEGER, false, false);
        public static final ColumnImpl AL_STRIPE_SIZE = new ColumnImpl("AL_STRIPE_SIZE", Types.BIGINT, false, false);
        public static final ColumnImpl TCP_PORT = new ColumnImpl("TCP_PORT", Types.INTEGER, false, true);
        public static final ColumnImpl TRANSPORT_TYPE = new ColumnImpl("TRANSPORT_TYPE", Types.VARCHAR, false, false);
        public static final ColumnImpl SECRET = new ColumnImpl("SECRET", Types.VARCHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            RESOURCE_NAME,
            RESOURCE_NAME_SUFFIX,
            SNAPSHOT_NAME,
            PEER_SLOTS,
            AL_STRIPES,
            AL_STRIPE_SIZE,
            TCP_PORT,
            TRANSPORT_TYPE,
            SECRET
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_DRBD_RESOURCE_DEFINITIONS";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_DRBD_RESOURCE_DEFINITIONS";
        }
    }

    public static class LayerDrbdVolumes implements DatabaseTable
    {
        private LayerDrbdVolumes()
        {
        }

        // Primary Keys
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
        public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            VLM_NR,
            NODE_NAME,
            POOL_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_DRBD_VOLUMES";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_DRBD_VOLUMES";
        }
    }

    public static class LayerDrbdVolumeDefinitions implements DatabaseTable
    {
        private LayerDrbdVolumeDefinitions()
        {
        }

        // Primary Keys
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl RESOURCE_NAME_SUFFIX = new ColumnImpl("RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl VLM_MINOR_NR = new ColumnImpl("VLM_MINOR_NR", Types.INTEGER, false, true);

        public static final Column[] ALL = new Column[]
        {
            RESOURCE_NAME,
            RESOURCE_NAME_SUFFIX,
            SNAPSHOT_NAME,
            VLM_NR,
            VLM_MINOR_NR
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_DRBD_VOLUME_DEFINITIONS";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_DRBD_VOLUME_DEFINITIONS";
        }
    }

    public static class LayerLuksVolumes implements DatabaseTable
    {
        private LayerLuksVolumes()
        {
        }

        // Primary Keys
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl ENCRYPTED_PASSWORD = new ColumnImpl("ENCRYPTED_PASSWORD", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            VLM_NR,
            ENCRYPTED_PASSWORD
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_LUKS_VOLUMES";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_LUKS_VOLUMES";
        }
    }

    public static class LayerResourceIds implements DatabaseTable
    {
        private LayerResourceIds()
        {
        }

        // Primary Key
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);

        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl LAYER_RESOURCE_KIND = new ColumnImpl("LAYER_RESOURCE_KIND", Types.VARCHAR, false, false);
        public static final ColumnImpl LAYER_RESOURCE_PARENT_ID = new ColumnImpl("LAYER_RESOURCE_PARENT_ID", Types.INTEGER, false, true);
        public static final ColumnImpl LAYER_RESOURCE_SUFFIX = new ColumnImpl("LAYER_RESOURCE_SUFFIX", Types.VARCHAR, false, false);
        public static final ColumnImpl LAYER_RESOURCE_SUSPENDED = new ColumnImpl("LAYER_RESOURCE_SUSPENDED", Types.BOOLEAN, false, false);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            NODE_NAME,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            LAYER_RESOURCE_KIND,
            LAYER_RESOURCE_PARENT_ID,
            LAYER_RESOURCE_SUFFIX,
            LAYER_RESOURCE_SUSPENDED
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_RESOURCE_IDS";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_RESOURCE_IDS";
        }
    }

    public static class LayerStorageVolumes implements DatabaseTable
    {
        private LayerStorageVolumes()
        {
        }

        // Primary Keys
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl PROVIDER_KIND = new ColumnImpl("PROVIDER_KIND", Types.VARCHAR, false, false);
        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl STOR_POOL_NAME = new ColumnImpl("STOR_POOL_NAME", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            VLM_NR,
            PROVIDER_KIND,
            NODE_NAME,
            STOR_POOL_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_STORAGE_VOLUMES";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_STORAGE_VOLUMES";
        }
    }

    public static class LayerWritecacheVolumes implements DatabaseTable
    {
        private LayerWritecacheVolumes()
        {
        }

        // Primary Keys
        public static final ColumnImpl LAYER_RESOURCE_ID = new ColumnImpl("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, false, true);
        public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            VLM_NR,
            NODE_NAME,
            POOL_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LAYER_WRITECACHE_VOLUMES";
        }

        @Override
        public String toString()
        {
            return "Table LAYER_WRITECACHE_VOLUMES";
        }
    }

    public static class LinstorRemotes implements DatabaseTable
    {
        private LinstorRemotes()
        {
        }

        // Primary Key
        public static final ColumnImpl NAME = new ColumnImpl("NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl DSP_NAME = new ColumnImpl("DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl URL = new ColumnImpl("URL", Types.VARCHAR, false, false);
        public static final ColumnImpl ENCRYPTED_PASSPHRASE = new ColumnImpl("ENCRYPTED_PASSPHRASE", Types.BLOB, false, true);
        public static final ColumnImpl CLUSTER_ID = new ColumnImpl("CLUSTER_ID", Types.CHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NAME,
            DSP_NAME,
            FLAGS,
            URL,
            ENCRYPTED_PASSPHRASE,
            CLUSTER_ID
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "LINSTOR_REMOTES";
        }

        @Override
        public String toString()
        {
            return "Table LINSTOR_REMOTES";
        }
    }

    public static class Nodes implements DatabaseTable
    {
        private Nodes()
        {
        }

        // Primary Key
        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl NODE_DSP_NAME = new ColumnImpl("NODE_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl NODE_FLAGS = new ColumnImpl("NODE_FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl NODE_TYPE = new ColumnImpl("NODE_TYPE", Types.INTEGER, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            NODE_DSP_NAME,
            NODE_FLAGS,
            NODE_TYPE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "NODES";
        }

        @Override
        public String toString()
        {
            return "Table NODES";
        }
    }

    public static class NodeConnections implements DatabaseTable
    {
        private NodeConnections()
        {
        }

        // Primary Keys
        public static final ColumnImpl NODE_NAME_SRC = new ColumnImpl("NODE_NAME_SRC", Types.VARCHAR, true, false);
        public static final ColumnImpl NODE_NAME_DST = new ColumnImpl("NODE_NAME_DST", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME_SRC,
            NODE_NAME_DST
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "NODE_CONNECTIONS";
        }

        @Override
        public String toString()
        {
            return "Table NODE_CONNECTIONS";
        }
    }

    public static class NodeNetInterfaces implements DatabaseTable
    {
        private NodeNetInterfaces()
        {
        }

        // Primary Keys
        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl NODE_NET_NAME = new ColumnImpl("NODE_NET_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl NODE_NET_DSP_NAME = new ColumnImpl("NODE_NET_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl INET_ADDRESS = new ColumnImpl("INET_ADDRESS", Types.VARCHAR, false, false);
        public static final ColumnImpl STLT_CONN_PORT = new ColumnImpl("STLT_CONN_PORT", Types.SMALLINT, false, true);
        public static final ColumnImpl STLT_CONN_ENCR_TYPE = new ColumnImpl("STLT_CONN_ENCR_TYPE", Types.VARCHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            NODE_NET_NAME,
            NODE_NET_DSP_NAME,
            INET_ADDRESS,
            STLT_CONN_PORT,
            STLT_CONN_ENCR_TYPE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "NODE_NET_INTERFACES";
        }

        @Override
        public String toString()
        {
            return "Table NODE_NET_INTERFACES";
        }
    }

    public static class NodeStorPool implements DatabaseTable
    {
        private NodeStorPool()
        {
        }

        // Primary Keys
        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl DRIVER_NAME = new ColumnImpl("DRIVER_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl FREE_SPACE_MGR_NAME = new ColumnImpl("FREE_SPACE_MGR_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl FREE_SPACE_MGR_DSP_NAME = new ColumnImpl("FREE_SPACE_MGR_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl EXTERNAL_LOCKING = new ColumnImpl("EXTERNAL_LOCKING", Types.BOOLEAN, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            POOL_NAME,
            DRIVER_NAME,
            FREE_SPACE_MGR_NAME,
            FREE_SPACE_MGR_DSP_NAME,
            EXTERNAL_LOCKING
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "NODE_STOR_POOL";
        }

        @Override
        public String toString()
        {
            return "Table NODE_STOR_POOL";
        }
    }

    public static class PropsContainers implements DatabaseTable
    {
        private PropsContainers()
        {
        }

        // Primary Keys
        public static final ColumnImpl PROPS_INSTANCE = new ColumnImpl("PROPS_INSTANCE", Types.VARCHAR, true, false);
        public static final ColumnImpl PROP_KEY = new ColumnImpl("PROP_KEY", Types.VARCHAR, true, false);

        public static final ColumnImpl PROP_VALUE = new ColumnImpl("PROP_VALUE", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            PROPS_INSTANCE,
            PROP_KEY,
            PROP_VALUE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "PROPS_CONTAINERS";
        }

        @Override
        public String toString()
        {
            return "Table PROPS_CONTAINERS";
        }
    }

    public static class Resources implements DatabaseTable
    {
        private Resources()
        {
        }

        // Primary Keys
        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl RESOURCE_FLAGS = new ColumnImpl("RESOURCE_FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl CREATE_TIMESTAMP = new ColumnImpl("CREATE_TIMESTAMP", Types.TIMESTAMP, false, true);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            RESOURCE_FLAGS,
            CREATE_TIMESTAMP
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "RESOURCES";
        }

        @Override
        public String toString()
        {
            return "Table RESOURCES";
        }
    }

    public static class ResourceConnections implements DatabaseTable
    {
        private ResourceConnections()
        {
        }

        // Primary Keys
        public static final ColumnImpl NODE_NAME_SRC = new ColumnImpl("NODE_NAME_SRC", Types.VARCHAR, true, false);
        public static final ColumnImpl NODE_NAME_DST = new ColumnImpl("NODE_NAME_DST", Types.VARCHAR, true, false);
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl TCP_PORT = new ColumnImpl("TCP_PORT", Types.INTEGER, false, true);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME_SRC,
            NODE_NAME_DST,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            FLAGS,
            TCP_PORT
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "RESOURCE_CONNECTIONS";
        }

        @Override
        public String toString()
        {
            return "Table RESOURCE_CONNECTIONS";
        }
    }

    public static class ResourceDefinitions implements DatabaseTable
    {
        private ResourceDefinitions()
        {
        }

        // Primary Keys
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl RESOURCE_DSP_NAME = new ColumnImpl("RESOURCE_DSP_NAME", Types.VARCHAR, false, true);
        public static final ColumnImpl SNAPSHOT_DSP_NAME = new ColumnImpl("SNAPSHOT_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl RESOURCE_FLAGS = new ColumnImpl("RESOURCE_FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl LAYER_STACK = new ColumnImpl("LAYER_STACK", Types.VARCHAR, false, false);
        public static final ColumnImpl RESOURCE_EXTERNAL_NAME = new ColumnImpl("RESOURCE_EXTERNAL_NAME", Types.BLOB, false, true);
        public static final ColumnImpl RESOURCE_GROUP_NAME = new ColumnImpl("RESOURCE_GROUP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl PARENT_UUID = new ColumnImpl("PARENT_UUID", Types.CHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            RESOURCE_DSP_NAME,
            SNAPSHOT_DSP_NAME,
            RESOURCE_FLAGS,
            LAYER_STACK,
            RESOURCE_EXTERNAL_NAME,
            RESOURCE_GROUP_NAME,
            PARENT_UUID
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "RESOURCE_DEFINITIONS";
        }

        @Override
        public String toString()
        {
            return "Table RESOURCE_DEFINITIONS";
        }
    }

    public static class ResourceGroups implements DatabaseTable
    {
        private ResourceGroups()
        {
        }

        // Primary Key
        public static final ColumnImpl RESOURCE_GROUP_NAME = new ColumnImpl("RESOURCE_GROUP_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl RESOURCE_GROUP_DSP_NAME = new ColumnImpl("RESOURCE_GROUP_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl DESCRIPTION = new ColumnImpl("DESCRIPTION", Types.VARCHAR, false, true);
        public static final ColumnImpl LAYER_STACK = new ColumnImpl("LAYER_STACK", Types.VARCHAR, false, true);
        public static final ColumnImpl REPLICA_COUNT = new ColumnImpl("REPLICA_COUNT", Types.INTEGER, false, false);
        public static final ColumnImpl NODE_NAME_LIST = new ColumnImpl("NODE_NAME_LIST", Types.CLOB, false, true);
        public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.CLOB, false, true);
        public static final ColumnImpl POOL_NAME_DISKLESS = new ColumnImpl("POOL_NAME_DISKLESS", Types.CLOB, false, true);
        public static final ColumnImpl DO_NOT_PLACE_WITH_RSC_REGEX = new ColumnImpl("DO_NOT_PLACE_WITH_RSC_REGEX", Types.CLOB, false, true);
        public static final ColumnImpl DO_NOT_PLACE_WITH_RSC_LIST = new ColumnImpl("DO_NOT_PLACE_WITH_RSC_LIST", Types.CLOB, false, true);
        public static final ColumnImpl REPLICAS_ON_SAME = new ColumnImpl("REPLICAS_ON_SAME", Types.CLOB, false, true);
        public static final ColumnImpl REPLICAS_ON_DIFFERENT = new ColumnImpl("REPLICAS_ON_DIFFERENT", Types.CLOB, false, true);
        public static final ColumnImpl X_REPLICAS_ON_DIFFERENT = new ColumnImpl("X_REPLICAS_ON_DIFFERENT", Types.CLOB, false, true);
        public static final ColumnImpl ALLOWED_PROVIDER_LIST = new ColumnImpl("ALLOWED_PROVIDER_LIST", Types.VARCHAR, false, true);
        public static final ColumnImpl DISKLESS_ON_REMAINING = new ColumnImpl("DISKLESS_ON_REMAINING", Types.BOOLEAN, false, true);
        public static final ColumnImpl PEER_SLOTS = new ColumnImpl("PEER_SLOTS", Types.SMALLINT, false, true);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_GROUP_NAME,
            RESOURCE_GROUP_DSP_NAME,
            DESCRIPTION,
            LAYER_STACK,
            REPLICA_COUNT,
            NODE_NAME_LIST,
            POOL_NAME,
            POOL_NAME_DISKLESS,
            DO_NOT_PLACE_WITH_RSC_REGEX,
            DO_NOT_PLACE_WITH_RSC_LIST,
            REPLICAS_ON_SAME,
            REPLICAS_ON_DIFFERENT,
            X_REPLICAS_ON_DIFFERENT,
            ALLOWED_PROVIDER_LIST,
            DISKLESS_ON_REMAINING,
            PEER_SLOTS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "RESOURCE_GROUPS";
        }

        @Override
        public String toString()
        {
            return "Table RESOURCE_GROUPS";
        }
    }

    public static class S3Remotes implements DatabaseTable
    {
        private S3Remotes()
        {
        }

        // Primary Key
        public static final ColumnImpl NAME = new ColumnImpl("NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl DSP_NAME = new ColumnImpl("DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl ENDPOINT = new ColumnImpl("ENDPOINT", Types.VARCHAR, false, false);
        public static final ColumnImpl BUCKET = new ColumnImpl("BUCKET", Types.VARCHAR, false, false);
        public static final ColumnImpl REGION = new ColumnImpl("REGION", Types.VARCHAR, false, false);
        public static final ColumnImpl ACCESS_KEY = new ColumnImpl("ACCESS_KEY", Types.BLOB, false, false);
        public static final ColumnImpl SECRET_KEY = new ColumnImpl("SECRET_KEY", Types.BLOB, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NAME,
            DSP_NAME,
            FLAGS,
            ENDPOINT,
            BUCKET,
            REGION,
            ACCESS_KEY,
            SECRET_KEY
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "S3_REMOTES";
        }

        @Override
        public String toString()
        {
            return "Table S3_REMOTES";
        }
    }

    public static class SatellitesCapacity implements DatabaseTable
    {
        private SatellitesCapacity()
        {
        }

        // Primary Key
        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl CAPACITY = new ColumnImpl("CAPACITY", Types.BLOB, false, false);
        public static final ColumnImpl FAIL_FLAG = new ColumnImpl("FAIL_FLAG", Types.BOOLEAN, false, false);
        public static final ColumnImpl ALLOCATED = new ColumnImpl("ALLOCATED", Types.BLOB, false, false);
        public static final ColumnImpl USABLE = new ColumnImpl("USABLE", Types.BLOB, false, false);

        public static final Column[] ALL = new Column[]
        {
            NODE_NAME,
            CAPACITY,
            FAIL_FLAG,
            ALLOCATED,
            USABLE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SATELLITES_CAPACITY";
        }

        @Override
        public String toString()
        {
            return "Table SATELLITES_CAPACITY";
        }
    }

    public static class Schedules implements DatabaseTable
    {
        private Schedules()
        {
        }

        // Primary Key
        public static final ColumnImpl NAME = new ColumnImpl("NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl DSP_NAME = new ColumnImpl("DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);
        public static final ColumnImpl FULL_CRON = new ColumnImpl("FULL_CRON", Types.VARCHAR, false, false);
        public static final ColumnImpl INC_CRON = new ColumnImpl("INC_CRON", Types.VARCHAR, false, true);
        public static final ColumnImpl KEEP_LOCAL = new ColumnImpl("KEEP_LOCAL", Types.INTEGER, false, true);
        public static final ColumnImpl KEEP_REMOTE = new ColumnImpl("KEEP_REMOTE", Types.INTEGER, false, true);
        public static final ColumnImpl ON_FAILURE = new ColumnImpl("ON_FAILURE", Types.BIGINT, false, false);
        public static final ColumnImpl MAX_RETRIES = new ColumnImpl("MAX_RETRIES", Types.INTEGER, false, true);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NAME,
            DSP_NAME,
            FLAGS,
            FULL_CRON,
            INC_CRON,
            KEEP_LOCAL,
            KEEP_REMOTE,
            ON_FAILURE,
            MAX_RETRIES
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SCHEDULES";
        }

        @Override
        public String toString()
        {
            return "Table SCHEDULES";
        }
    }

    public static class SecAccessTypes implements DatabaseTable
    {
        private SecAccessTypes()
        {
        }

        // Primary Key
        public static final ColumnImpl ACCESS_TYPE_NAME = new ColumnImpl("ACCESS_TYPE_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl ACCESS_TYPE_VALUE = new ColumnImpl("ACCESS_TYPE_VALUE", Types.SMALLINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            ACCESS_TYPE_NAME,
            ACCESS_TYPE_VALUE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_ACCESS_TYPES";
        }

        @Override
        public String toString()
        {
            return "Table SEC_ACCESS_TYPES";
        }
    }

    public static class SecAclMap implements DatabaseTable
    {
        private SecAclMap()
        {
        }

        // Primary Keys
        public static final ColumnImpl OBJECT_PATH = new ColumnImpl("OBJECT_PATH", Types.VARCHAR, true, false);
        public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl ACCESS_TYPE = new ColumnImpl("ACCESS_TYPE", Types.SMALLINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            OBJECT_PATH,
            ROLE_NAME,
            ACCESS_TYPE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_ACL_MAP";
        }

        @Override
        public String toString()
        {
            return "Table SEC_ACL_MAP";
        }
    }

    public static class SecConfiguration implements DatabaseTable
    {
        private SecConfiguration()
        {
        }

        // Primary Key
        public static final ColumnImpl ENTRY_KEY = new ColumnImpl("ENTRY_KEY", Types.VARCHAR, true, false);

        public static final ColumnImpl ENTRY_DSP_KEY = new ColumnImpl("ENTRY_DSP_KEY", Types.VARCHAR, false, false);
        public static final ColumnImpl ENTRY_VALUE = new ColumnImpl("ENTRY_VALUE", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            ENTRY_KEY,
            ENTRY_DSP_KEY,
            ENTRY_VALUE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_CONFIGURATION";
        }

        @Override
        public String toString()
        {
            return "Table SEC_CONFIGURATION";
        }
    }

    public static class SecDfltRoles implements DatabaseTable
    {
        private SecDfltRoles()
        {
        }

        // Primary Key
        public static final ColumnImpl IDENTITY_NAME = new ColumnImpl("IDENTITY_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            IDENTITY_NAME,
            ROLE_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_DFLT_ROLES";
        }

        @Override
        public String toString()
        {
            return "Table SEC_DFLT_ROLES";
        }
    }

    public static class SecIdentities implements DatabaseTable
    {
        private SecIdentities()
        {
        }

        // Primary Key
        public static final ColumnImpl IDENTITY_NAME = new ColumnImpl("IDENTITY_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl IDENTITY_DSP_NAME = new ColumnImpl("IDENTITY_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl PASS_SALT = new ColumnImpl("PASS_SALT", Types.CHAR, false, true);
        public static final ColumnImpl PASS_HASH = new ColumnImpl("PASS_HASH", Types.CHAR, false, true);
        public static final ColumnImpl ID_ENABLED = new ColumnImpl("ID_ENABLED", Types.BOOLEAN, false, false);
        public static final ColumnImpl ID_LOCKED = new ColumnImpl("ID_LOCKED", Types.BOOLEAN, false, false);

        public static final Column[] ALL = new Column[]
        {
            IDENTITY_NAME,
            IDENTITY_DSP_NAME,
            PASS_SALT,
            PASS_HASH,
            ID_ENABLED,
            ID_LOCKED
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_IDENTITIES";
        }

        @Override
        public String toString()
        {
            return "Table SEC_IDENTITIES";
        }
    }

    public static class SecIdRoleMap implements DatabaseTable
    {
        private SecIdRoleMap()
        {
        }

        // Primary Keys
        public static final ColumnImpl IDENTITY_NAME = new ColumnImpl("IDENTITY_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, true, false);

        public static final Column[] ALL = new Column[]
        {
            IDENTITY_NAME,
            ROLE_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_ID_ROLE_MAP";
        }

        @Override
        public String toString()
        {
            return "Table SEC_ID_ROLE_MAP";
        }
    }

    public static class SecObjectProtection implements DatabaseTable
    {
        private SecObjectProtection()
        {
        }

        // Primary Key
        public static final ColumnImpl OBJECT_PATH = new ColumnImpl("OBJECT_PATH", Types.VARCHAR, true, false);

        public static final ColumnImpl CREATOR_IDENTITY_NAME = new ColumnImpl("CREATOR_IDENTITY_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl OWNER_ROLE_NAME = new ColumnImpl("OWNER_ROLE_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl SECURITY_TYPE_NAME = new ColumnImpl("SECURITY_TYPE_NAME", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            OBJECT_PATH,
            CREATOR_IDENTITY_NAME,
            OWNER_ROLE_NAME,
            SECURITY_TYPE_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_OBJECT_PROTECTION";
        }

        @Override
        public String toString()
        {
            return "Table SEC_OBJECT_PROTECTION";
        }
    }

    public static class SecRoles implements DatabaseTable
    {
        private SecRoles()
        {
        }

        // Primary Key
        public static final ColumnImpl ROLE_NAME = new ColumnImpl("ROLE_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl ROLE_DSP_NAME = new ColumnImpl("ROLE_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl DOMAIN_NAME = new ColumnImpl("DOMAIN_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl ROLE_ENABLED = new ColumnImpl("ROLE_ENABLED", Types.BOOLEAN, false, false);
        public static final ColumnImpl ROLE_PRIVILEGES = new ColumnImpl("ROLE_PRIVILEGES", Types.BIGINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            ROLE_NAME,
            ROLE_DSP_NAME,
            DOMAIN_NAME,
            ROLE_ENABLED,
            ROLE_PRIVILEGES
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_ROLES";
        }

        @Override
        public String toString()
        {
            return "Table SEC_ROLES";
        }
    }

    public static class SecTypes implements DatabaseTable
    {
        private SecTypes()
        {
        }

        // Primary Key
        public static final ColumnImpl TYPE_NAME = new ColumnImpl("TYPE_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl TYPE_DSP_NAME = new ColumnImpl("TYPE_DSP_NAME", Types.VARCHAR, false, false);
        public static final ColumnImpl TYPE_ENABLED = new ColumnImpl("TYPE_ENABLED", Types.BOOLEAN, false, false);

        public static final Column[] ALL = new Column[]
        {
            TYPE_NAME,
            TYPE_DSP_NAME,
            TYPE_ENABLED
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_TYPES";
        }

        @Override
        public String toString()
        {
            return "Table SEC_TYPES";
        }
    }

    public static class SecTypeRules implements DatabaseTable
    {
        private SecTypeRules()
        {
        }

        // Primary Keys
        public static final ColumnImpl DOMAIN_NAME = new ColumnImpl("DOMAIN_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl TYPE_NAME = new ColumnImpl("TYPE_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl ACCESS_TYPE = new ColumnImpl("ACCESS_TYPE", Types.SMALLINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            DOMAIN_NAME,
            TYPE_NAME,
            ACCESS_TYPE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SEC_TYPE_RULES";
        }

        @Override
        public String toString()
        {
            return "Table SEC_TYPE_RULES";
        }
    }

    public static class SpaceHistory implements DatabaseTable
    {
        private SpaceHistory()
        {
        }

        // Primary Key
        public static final ColumnImpl ENTRY_DATE = new ColumnImpl("ENTRY_DATE", Types.DATE, true, false);

        public static final ColumnImpl CAPACITY = new ColumnImpl("CAPACITY", Types.BLOB, false, false);

        public static final Column[] ALL = new Column[]
        {
            ENTRY_DATE,
            CAPACITY
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "SPACE_HISTORY";
        }

        @Override
        public String toString()
        {
            return "Table SPACE_HISTORY";
        }
    }

    public static class StorPoolDefinitions implements DatabaseTable
    {
        private StorPoolDefinitions()
        {
        }

        // Primary Key
        public static final ColumnImpl POOL_NAME = new ColumnImpl("POOL_NAME", Types.VARCHAR, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl POOL_DSP_NAME = new ColumnImpl("POOL_DSP_NAME", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            POOL_NAME,
            POOL_DSP_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "STOR_POOL_DEFINITIONS";
        }

        @Override
        public String toString()
        {
            return "Table STOR_POOL_DEFINITIONS";
        }
    }

    public static class TrackingDate implements DatabaseTable
    {
        private TrackingDate()
        {
        }

        // Primary Key

        public static final ColumnImpl ENTRY_DATE = new ColumnImpl("ENTRY_DATE", Types.DATE, false, false);

        public static final Column[] ALL = new Column[]
        {
            ENTRY_DATE
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "TRACKING_DATE";
        }

        @Override
        public String toString()
        {
            return "Table TRACKING_DATE";
        }
    }

    public static class Volumes implements DatabaseTable
    {
        private Volumes()
        {
        }

        // Primary Keys
        public static final ColumnImpl NODE_NAME = new ColumnImpl("NODE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl VLM_FLAGS = new ColumnImpl("VLM_FLAGS", Types.BIGINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            VLM_NR,
            VLM_FLAGS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "VOLUMES";
        }

        @Override
        public String toString()
        {
            return "Table VOLUMES";
        }
    }

    public static class VolumeConnections implements DatabaseTable
    {
        private VolumeConnections()
        {
        }

        // Primary Keys
        public static final ColumnImpl NODE_NAME_SRC = new ColumnImpl("NODE_NAME_SRC", Types.VARCHAR, true, false);
        public static final ColumnImpl NODE_NAME_DST = new ColumnImpl("NODE_NAME_DST", Types.VARCHAR, true, false);
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME_SRC,
            NODE_NAME_DST,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            VLM_NR
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "VOLUME_CONNECTIONS";
        }

        @Override
        public String toString()
        {
            return "Table VOLUME_CONNECTIONS";
        }
    }

    public static class VolumeDefinitions implements DatabaseTable
    {
        private VolumeDefinitions()
        {
        }

        // Primary Keys
        public static final ColumnImpl RESOURCE_NAME = new ColumnImpl("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl SNAPSHOT_NAME = new ColumnImpl("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl VLM_SIZE = new ColumnImpl("VLM_SIZE", Types.BIGINT, false, false);
        public static final ColumnImpl VLM_FLAGS = new ColumnImpl("VLM_FLAGS", Types.BIGINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            VLM_NR,
            VLM_SIZE,
            VLM_FLAGS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "VOLUME_DEFINITIONS";
        }

        @Override
        public String toString()
        {
            return "Table VOLUME_DEFINITIONS";
        }
    }

    public static class VolumeGroups implements DatabaseTable
    {
        private VolumeGroups()
        {
        }

        // Primary Keys
        public static final ColumnImpl RESOURCE_GROUP_NAME = new ColumnImpl("RESOURCE_GROUP_NAME", Types.VARCHAR, true, false);
        public static final ColumnImpl VLM_NR = new ColumnImpl("VLM_NR", Types.INTEGER, true, false);

        public static final ColumnImpl UUID = new ColumnImpl("UUID", Types.CHAR, false, false);
        public static final ColumnImpl FLAGS = new ColumnImpl("FLAGS", Types.BIGINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_GROUP_NAME,
            VLM_NR,
            FLAGS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String getName()
        {
            return "VOLUME_GROUPS";
        }

        @Override
        public String toString()
        {
            return "Table VOLUME_GROUPS";
        }
    }

    public static final DatabaseTable[] ALL_TABLES; // initialized in static block
    public static final EbsRemotes EBS_REMOTES = new EbsRemotes();
    public static final Files FILES = new Files();
    public static final KeyValueStore KEY_VALUE_STORE = new KeyValueStore();
    public static final LayerBcacheVolumes LAYER_BCACHE_VOLUMES = new LayerBcacheVolumes();
    public static final LayerCacheVolumes LAYER_CACHE_VOLUMES = new LayerCacheVolumes();
    public static final LayerDrbdResources LAYER_DRBD_RESOURCES = new LayerDrbdResources();
    public static final LayerDrbdResourceDefinitions LAYER_DRBD_RESOURCE_DEFINITIONS = new LayerDrbdResourceDefinitions();
    public static final LayerDrbdVolumes LAYER_DRBD_VOLUMES = new LayerDrbdVolumes();
    public static final LayerDrbdVolumeDefinitions LAYER_DRBD_VOLUME_DEFINITIONS = new LayerDrbdVolumeDefinitions();
    public static final LayerLuksVolumes LAYER_LUKS_VOLUMES = new LayerLuksVolumes();
    public static final LayerResourceIds LAYER_RESOURCE_IDS = new LayerResourceIds();
    public static final LayerStorageVolumes LAYER_STORAGE_VOLUMES = new LayerStorageVolumes();
    public static final LayerWritecacheVolumes LAYER_WRITECACHE_VOLUMES = new LayerWritecacheVolumes();
    public static final LinstorRemotes LINSTOR_REMOTES = new LinstorRemotes();
    public static final Nodes NODES = new Nodes();
    public static final NodeConnections NODE_CONNECTIONS = new NodeConnections();
    public static final NodeNetInterfaces NODE_NET_INTERFACES = new NodeNetInterfaces();
    public static final NodeStorPool NODE_STOR_POOL = new NodeStorPool();
    public static final PropsContainers PROPS_CONTAINERS = new PropsContainers();
    public static final Resources RESOURCES = new Resources();
    public static final ResourceConnections RESOURCE_CONNECTIONS = new ResourceConnections();
    public static final ResourceDefinitions RESOURCE_DEFINITIONS = new ResourceDefinitions();
    public static final ResourceGroups RESOURCE_GROUPS = new ResourceGroups();
    public static final S3Remotes S3_REMOTES = new S3Remotes();
    public static final SatellitesCapacity SATELLITES_CAPACITY = new SatellitesCapacity();
    public static final Schedules SCHEDULES = new Schedules();
    public static final SecAccessTypes SEC_ACCESS_TYPES = new SecAccessTypes();
    public static final SecAclMap SEC_ACL_MAP = new SecAclMap();
    public static final SecConfiguration SEC_CONFIGURATION = new SecConfiguration();
    public static final SecDfltRoles SEC_DFLT_ROLES = new SecDfltRoles();
    public static final SecIdentities SEC_IDENTITIES = new SecIdentities();
    public static final SecIdRoleMap SEC_ID_ROLE_MAP = new SecIdRoleMap();
    public static final SecObjectProtection SEC_OBJECT_PROTECTION = new SecObjectProtection();
    public static final SecRoles SEC_ROLES = new SecRoles();
    public static final SecTypes SEC_TYPES = new SecTypes();
    public static final SecTypeRules SEC_TYPE_RULES = new SecTypeRules();
    public static final SpaceHistory SPACE_HISTORY = new SpaceHistory();
    public static final StorPoolDefinitions STOR_POOL_DEFINITIONS = new StorPoolDefinitions();
    public static final TrackingDate TRACKING_DATE = new TrackingDate();
    public static final Volumes VOLUMES = new Volumes();
    public static final VolumeConnections VOLUME_CONNECTIONS = new VolumeConnections();
    public static final VolumeDefinitions VOLUME_DEFINITIONS = new VolumeDefinitions();
    public static final VolumeGroups VOLUME_GROUPS = new VolumeGroups();

    static
    {
        ALL_TABLES = new DatabaseTable[] {
            EBS_REMOTES,
            FILES,
            KEY_VALUE_STORE,
            NODES,
            RESOURCE_DEFINITIONS,
            RESOURCES,
            LAYER_RESOURCE_IDS,
            STOR_POOL_DEFINITIONS,
            NODE_STOR_POOL,
            LAYER_BCACHE_VOLUMES,
            LAYER_CACHE_VOLUMES,
            LAYER_DRBD_RESOURCES,
            LAYER_DRBD_RESOURCE_DEFINITIONS,
            LAYER_DRBD_VOLUMES,
            VOLUME_DEFINITIONS,
            LAYER_DRBD_VOLUME_DEFINITIONS,
            LAYER_LUKS_VOLUMES,
            LAYER_STORAGE_VOLUMES,
            LAYER_WRITECACHE_VOLUMES,
            LINSTOR_REMOTES,
            NODE_CONNECTIONS,
            NODE_NET_INTERFACES,
            PROPS_CONTAINERS,
            RESOURCE_CONNECTIONS,
            S3_REMOTES,
            SATELLITES_CAPACITY,
            SCHEDULES,
            SEC_ACCESS_TYPES,
            SEC_IDENTITIES,
            SEC_TYPES,
            SEC_ROLES,
            SEC_OBJECT_PROTECTION,
            SEC_ACL_MAP,
            SEC_CONFIGURATION,
            SEC_ID_ROLE_MAP,
            SEC_DFLT_ROLES,
            SEC_TYPE_RULES,
            SPACE_HISTORY,
            TRACKING_DATE,
            VOLUMES,
            VOLUME_CONNECTIONS,
            RESOURCE_GROUPS,
            VOLUME_GROUPS
        };

        EbsRemotes.UUID.table = EBS_REMOTES;
        EbsRemotes.NAME.table = EBS_REMOTES;
        EbsRemotes.DSP_NAME.table = EBS_REMOTES;
        EbsRemotes.FLAGS.table = EBS_REMOTES;
        EbsRemotes.URL.table = EBS_REMOTES;
        EbsRemotes.REGION.table = EBS_REMOTES;
        EbsRemotes.AVAILABILITY_ZONE.table = EBS_REMOTES;
        EbsRemotes.ACCESS_KEY.table = EBS_REMOTES;
        EbsRemotes.SECRET_KEY.table = EBS_REMOTES;
        Files.UUID.table = FILES;
        Files.PATH.table = FILES;
        Files.FLAGS.table = FILES;
        Files.CONTENT.table = FILES;
        Files.CONTENT_CHECKSUM.table = FILES;
        KeyValueStore.UUID.table = KEY_VALUE_STORE;
        KeyValueStore.KVS_NAME.table = KEY_VALUE_STORE;
        KeyValueStore.KVS_DSP_NAME.table = KEY_VALUE_STORE;
        LayerBcacheVolumes.LAYER_RESOURCE_ID.table = LAYER_BCACHE_VOLUMES;
        LayerBcacheVolumes.VLM_NR.table = LAYER_BCACHE_VOLUMES;
        LayerBcacheVolumes.NODE_NAME.table = LAYER_BCACHE_VOLUMES;
        LayerBcacheVolumes.POOL_NAME.table = LAYER_BCACHE_VOLUMES;
        LayerBcacheVolumes.DEV_UUID.table = LAYER_BCACHE_VOLUMES;
        LayerCacheVolumes.LAYER_RESOURCE_ID.table = LAYER_CACHE_VOLUMES;
        LayerCacheVolumes.VLM_NR.table = LAYER_CACHE_VOLUMES;
        LayerCacheVolumes.NODE_NAME.table = LAYER_CACHE_VOLUMES;
        LayerCacheVolumes.POOL_NAME_CACHE.table = LAYER_CACHE_VOLUMES;
        LayerCacheVolumes.POOL_NAME_META.table = LAYER_CACHE_VOLUMES;
        LayerDrbdResources.LAYER_RESOURCE_ID.table = LAYER_DRBD_RESOURCES;
        LayerDrbdResources.PEER_SLOTS.table = LAYER_DRBD_RESOURCES;
        LayerDrbdResources.AL_STRIPES.table = LAYER_DRBD_RESOURCES;
        LayerDrbdResources.AL_STRIPE_SIZE.table = LAYER_DRBD_RESOURCES;
        LayerDrbdResources.FLAGS.table = LAYER_DRBD_RESOURCES;
        LayerDrbdResources.NODE_ID.table = LAYER_DRBD_RESOURCES;
        LayerDrbdResourceDefinitions.RESOURCE_NAME.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.RESOURCE_NAME_SUFFIX.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.SNAPSHOT_NAME.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.PEER_SLOTS.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.AL_STRIPES.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.AL_STRIPE_SIZE.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.TCP_PORT.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.TRANSPORT_TYPE.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdResourceDefinitions.SECRET.table = LAYER_DRBD_RESOURCE_DEFINITIONS;
        LayerDrbdVolumes.LAYER_RESOURCE_ID.table = LAYER_DRBD_VOLUMES;
        LayerDrbdVolumes.VLM_NR.table = LAYER_DRBD_VOLUMES;
        LayerDrbdVolumes.NODE_NAME.table = LAYER_DRBD_VOLUMES;
        LayerDrbdVolumes.POOL_NAME.table = LAYER_DRBD_VOLUMES;
        LayerDrbdVolumeDefinitions.RESOURCE_NAME.table = LAYER_DRBD_VOLUME_DEFINITIONS;
        LayerDrbdVolumeDefinitions.RESOURCE_NAME_SUFFIX.table = LAYER_DRBD_VOLUME_DEFINITIONS;
        LayerDrbdVolumeDefinitions.SNAPSHOT_NAME.table = LAYER_DRBD_VOLUME_DEFINITIONS;
        LayerDrbdVolumeDefinitions.VLM_NR.table = LAYER_DRBD_VOLUME_DEFINITIONS;
        LayerDrbdVolumeDefinitions.VLM_MINOR_NR.table = LAYER_DRBD_VOLUME_DEFINITIONS;
        LayerLuksVolumes.LAYER_RESOURCE_ID.table = LAYER_LUKS_VOLUMES;
        LayerLuksVolumes.VLM_NR.table = LAYER_LUKS_VOLUMES;
        LayerLuksVolumes.ENCRYPTED_PASSWORD.table = LAYER_LUKS_VOLUMES;
        LayerResourceIds.LAYER_RESOURCE_ID.table = LAYER_RESOURCE_IDS;
        LayerResourceIds.NODE_NAME.table = LAYER_RESOURCE_IDS;
        LayerResourceIds.RESOURCE_NAME.table = LAYER_RESOURCE_IDS;
        LayerResourceIds.SNAPSHOT_NAME.table = LAYER_RESOURCE_IDS;
        LayerResourceIds.LAYER_RESOURCE_KIND.table = LAYER_RESOURCE_IDS;
        LayerResourceIds.LAYER_RESOURCE_PARENT_ID.table = LAYER_RESOURCE_IDS;
        LayerResourceIds.LAYER_RESOURCE_SUFFIX.table = LAYER_RESOURCE_IDS;
        LayerResourceIds.LAYER_RESOURCE_SUSPENDED.table = LAYER_RESOURCE_IDS;
        LayerStorageVolumes.LAYER_RESOURCE_ID.table = LAYER_STORAGE_VOLUMES;
        LayerStorageVolumes.VLM_NR.table = LAYER_STORAGE_VOLUMES;
        LayerStorageVolumes.PROVIDER_KIND.table = LAYER_STORAGE_VOLUMES;
        LayerStorageVolumes.NODE_NAME.table = LAYER_STORAGE_VOLUMES;
        LayerStorageVolumes.STOR_POOL_NAME.table = LAYER_STORAGE_VOLUMES;
        LayerWritecacheVolumes.LAYER_RESOURCE_ID.table = LAYER_WRITECACHE_VOLUMES;
        LayerWritecacheVolumes.VLM_NR.table = LAYER_WRITECACHE_VOLUMES;
        LayerWritecacheVolumes.NODE_NAME.table = LAYER_WRITECACHE_VOLUMES;
        LayerWritecacheVolumes.POOL_NAME.table = LAYER_WRITECACHE_VOLUMES;
        LinstorRemotes.UUID.table = LINSTOR_REMOTES;
        LinstorRemotes.NAME.table = LINSTOR_REMOTES;
        LinstorRemotes.DSP_NAME.table = LINSTOR_REMOTES;
        LinstorRemotes.FLAGS.table = LINSTOR_REMOTES;
        LinstorRemotes.URL.table = LINSTOR_REMOTES;
        LinstorRemotes.ENCRYPTED_PASSPHRASE.table = LINSTOR_REMOTES;
        LinstorRemotes.CLUSTER_ID.table = LINSTOR_REMOTES;
        Nodes.UUID.table = NODES;
        Nodes.NODE_NAME.table = NODES;
        Nodes.NODE_DSP_NAME.table = NODES;
        Nodes.NODE_FLAGS.table = NODES;
        Nodes.NODE_TYPE.table = NODES;
        NodeConnections.UUID.table = NODE_CONNECTIONS;
        NodeConnections.NODE_NAME_SRC.table = NODE_CONNECTIONS;
        NodeConnections.NODE_NAME_DST.table = NODE_CONNECTIONS;
        NodeNetInterfaces.UUID.table = NODE_NET_INTERFACES;
        NodeNetInterfaces.NODE_NAME.table = NODE_NET_INTERFACES;
        NodeNetInterfaces.NODE_NET_NAME.table = NODE_NET_INTERFACES;
        NodeNetInterfaces.NODE_NET_DSP_NAME.table = NODE_NET_INTERFACES;
        NodeNetInterfaces.INET_ADDRESS.table = NODE_NET_INTERFACES;
        NodeNetInterfaces.STLT_CONN_PORT.table = NODE_NET_INTERFACES;
        NodeNetInterfaces.STLT_CONN_ENCR_TYPE.table = NODE_NET_INTERFACES;
        NodeStorPool.UUID.table = NODE_STOR_POOL;
        NodeStorPool.NODE_NAME.table = NODE_STOR_POOL;
        NodeStorPool.POOL_NAME.table = NODE_STOR_POOL;
        NodeStorPool.DRIVER_NAME.table = NODE_STOR_POOL;
        NodeStorPool.FREE_SPACE_MGR_NAME.table = NODE_STOR_POOL;
        NodeStorPool.FREE_SPACE_MGR_DSP_NAME.table = NODE_STOR_POOL;
        NodeStorPool.EXTERNAL_LOCKING.table = NODE_STOR_POOL;
        PropsContainers.PROPS_INSTANCE.table = PROPS_CONTAINERS;
        PropsContainers.PROP_KEY.table = PROPS_CONTAINERS;
        PropsContainers.PROP_VALUE.table = PROPS_CONTAINERS;
        Resources.UUID.table = RESOURCES;
        Resources.NODE_NAME.table = RESOURCES;
        Resources.RESOURCE_NAME.table = RESOURCES;
        Resources.SNAPSHOT_NAME.table = RESOURCES;
        Resources.RESOURCE_FLAGS.table = RESOURCES;
        Resources.CREATE_TIMESTAMP.table = RESOURCES;
        ResourceConnections.UUID.table = RESOURCE_CONNECTIONS;
        ResourceConnections.NODE_NAME_SRC.table = RESOURCE_CONNECTIONS;
        ResourceConnections.NODE_NAME_DST.table = RESOURCE_CONNECTIONS;
        ResourceConnections.RESOURCE_NAME.table = RESOURCE_CONNECTIONS;
        ResourceConnections.SNAPSHOT_NAME.table = RESOURCE_CONNECTIONS;
        ResourceConnections.FLAGS.table = RESOURCE_CONNECTIONS;
        ResourceConnections.TCP_PORT.table = RESOURCE_CONNECTIONS;
        ResourceDefinitions.UUID.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.RESOURCE_NAME.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.SNAPSHOT_NAME.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.RESOURCE_DSP_NAME.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.SNAPSHOT_DSP_NAME.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.RESOURCE_FLAGS.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.LAYER_STACK.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.RESOURCE_EXTERNAL_NAME.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.RESOURCE_GROUP_NAME.table = RESOURCE_DEFINITIONS;
        ResourceDefinitions.PARENT_UUID.table = RESOURCE_DEFINITIONS;
        ResourceGroups.UUID.table = RESOURCE_GROUPS;
        ResourceGroups.RESOURCE_GROUP_NAME.table = RESOURCE_GROUPS;
        ResourceGroups.RESOURCE_GROUP_DSP_NAME.table = RESOURCE_GROUPS;
        ResourceGroups.DESCRIPTION.table = RESOURCE_GROUPS;
        ResourceGroups.LAYER_STACK.table = RESOURCE_GROUPS;
        ResourceGroups.REPLICA_COUNT.table = RESOURCE_GROUPS;
        ResourceGroups.NODE_NAME_LIST.table = RESOURCE_GROUPS;
        ResourceGroups.POOL_NAME.table = RESOURCE_GROUPS;
        ResourceGroups.POOL_NAME_DISKLESS.table = RESOURCE_GROUPS;
        ResourceGroups.DO_NOT_PLACE_WITH_RSC_REGEX.table = RESOURCE_GROUPS;
        ResourceGroups.DO_NOT_PLACE_WITH_RSC_LIST.table = RESOURCE_GROUPS;
        ResourceGroups.REPLICAS_ON_SAME.table = RESOURCE_GROUPS;
        ResourceGroups.REPLICAS_ON_DIFFERENT.table = RESOURCE_GROUPS;
        ResourceGroups.X_REPLICAS_ON_DIFFERENT.table = RESOURCE_GROUPS;
        ResourceGroups.ALLOWED_PROVIDER_LIST.table = RESOURCE_GROUPS;
        ResourceGroups.DISKLESS_ON_REMAINING.table = RESOURCE_GROUPS;
        ResourceGroups.PEER_SLOTS.table = RESOURCE_GROUPS;
        S3Remotes.UUID.table = S3_REMOTES;
        S3Remotes.NAME.table = S3_REMOTES;
        S3Remotes.DSP_NAME.table = S3_REMOTES;
        S3Remotes.FLAGS.table = S3_REMOTES;
        S3Remotes.ENDPOINT.table = S3_REMOTES;
        S3Remotes.BUCKET.table = S3_REMOTES;
        S3Remotes.REGION.table = S3_REMOTES;
        S3Remotes.ACCESS_KEY.table = S3_REMOTES;
        S3Remotes.SECRET_KEY.table = S3_REMOTES;
        SatellitesCapacity.NODE_NAME.table = SATELLITES_CAPACITY;
        SatellitesCapacity.CAPACITY.table = SATELLITES_CAPACITY;
        SatellitesCapacity.FAIL_FLAG.table = SATELLITES_CAPACITY;
        SatellitesCapacity.ALLOCATED.table = SATELLITES_CAPACITY;
        SatellitesCapacity.USABLE.table = SATELLITES_CAPACITY;
        Schedules.UUID.table = SCHEDULES;
        Schedules.NAME.table = SCHEDULES;
        Schedules.DSP_NAME.table = SCHEDULES;
        Schedules.FLAGS.table = SCHEDULES;
        Schedules.FULL_CRON.table = SCHEDULES;
        Schedules.INC_CRON.table = SCHEDULES;
        Schedules.KEEP_LOCAL.table = SCHEDULES;
        Schedules.KEEP_REMOTE.table = SCHEDULES;
        Schedules.ON_FAILURE.table = SCHEDULES;
        Schedules.MAX_RETRIES.table = SCHEDULES;
        SecAccessTypes.ACCESS_TYPE_NAME.table = SEC_ACCESS_TYPES;
        SecAccessTypes.ACCESS_TYPE_VALUE.table = SEC_ACCESS_TYPES;
        SecAclMap.OBJECT_PATH.table = SEC_ACL_MAP;
        SecAclMap.ROLE_NAME.table = SEC_ACL_MAP;
        SecAclMap.ACCESS_TYPE.table = SEC_ACL_MAP;
        SecConfiguration.ENTRY_KEY.table = SEC_CONFIGURATION;
        SecConfiguration.ENTRY_DSP_KEY.table = SEC_CONFIGURATION;
        SecConfiguration.ENTRY_VALUE.table = SEC_CONFIGURATION;
        SecDfltRoles.IDENTITY_NAME.table = SEC_DFLT_ROLES;
        SecDfltRoles.ROLE_NAME.table = SEC_DFLT_ROLES;
        SecIdentities.IDENTITY_NAME.table = SEC_IDENTITIES;
        SecIdentities.IDENTITY_DSP_NAME.table = SEC_IDENTITIES;
        SecIdentities.PASS_SALT.table = SEC_IDENTITIES;
        SecIdentities.PASS_HASH.table = SEC_IDENTITIES;
        SecIdentities.ID_ENABLED.table = SEC_IDENTITIES;
        SecIdentities.ID_LOCKED.table = SEC_IDENTITIES;
        SecIdRoleMap.IDENTITY_NAME.table = SEC_ID_ROLE_MAP;
        SecIdRoleMap.ROLE_NAME.table = SEC_ID_ROLE_MAP;
        SecObjectProtection.OBJECT_PATH.table = SEC_OBJECT_PROTECTION;
        SecObjectProtection.CREATOR_IDENTITY_NAME.table = SEC_OBJECT_PROTECTION;
        SecObjectProtection.OWNER_ROLE_NAME.table = SEC_OBJECT_PROTECTION;
        SecObjectProtection.SECURITY_TYPE_NAME.table = SEC_OBJECT_PROTECTION;
        SecRoles.ROLE_NAME.table = SEC_ROLES;
        SecRoles.ROLE_DSP_NAME.table = SEC_ROLES;
        SecRoles.DOMAIN_NAME.table = SEC_ROLES;
        SecRoles.ROLE_ENABLED.table = SEC_ROLES;
        SecRoles.ROLE_PRIVILEGES.table = SEC_ROLES;
        SecTypes.TYPE_NAME.table = SEC_TYPES;
        SecTypes.TYPE_DSP_NAME.table = SEC_TYPES;
        SecTypes.TYPE_ENABLED.table = SEC_TYPES;
        SecTypeRules.DOMAIN_NAME.table = SEC_TYPE_RULES;
        SecTypeRules.TYPE_NAME.table = SEC_TYPE_RULES;
        SecTypeRules.ACCESS_TYPE.table = SEC_TYPE_RULES;
        SpaceHistory.ENTRY_DATE.table = SPACE_HISTORY;
        SpaceHistory.CAPACITY.table = SPACE_HISTORY;
        StorPoolDefinitions.UUID.table = STOR_POOL_DEFINITIONS;
        StorPoolDefinitions.POOL_NAME.table = STOR_POOL_DEFINITIONS;
        StorPoolDefinitions.POOL_DSP_NAME.table = STOR_POOL_DEFINITIONS;
        TrackingDate.ENTRY_DATE.table = TRACKING_DATE;
        Volumes.UUID.table = VOLUMES;
        Volumes.NODE_NAME.table = VOLUMES;
        Volumes.RESOURCE_NAME.table = VOLUMES;
        Volumes.SNAPSHOT_NAME.table = VOLUMES;
        Volumes.VLM_NR.table = VOLUMES;
        Volumes.VLM_FLAGS.table = VOLUMES;
        VolumeConnections.UUID.table = VOLUME_CONNECTIONS;
        VolumeConnections.NODE_NAME_SRC.table = VOLUME_CONNECTIONS;
        VolumeConnections.NODE_NAME_DST.table = VOLUME_CONNECTIONS;
        VolumeConnections.RESOURCE_NAME.table = VOLUME_CONNECTIONS;
        VolumeConnections.SNAPSHOT_NAME.table = VOLUME_CONNECTIONS;
        VolumeConnections.VLM_NR.table = VOLUME_CONNECTIONS;
        VolumeDefinitions.UUID.table = VOLUME_DEFINITIONS;
        VolumeDefinitions.RESOURCE_NAME.table = VOLUME_DEFINITIONS;
        VolumeDefinitions.SNAPSHOT_NAME.table = VOLUME_DEFINITIONS;
        VolumeDefinitions.VLM_NR.table = VOLUME_DEFINITIONS;
        VolumeDefinitions.VLM_SIZE.table = VOLUME_DEFINITIONS;
        VolumeDefinitions.VLM_FLAGS.table = VOLUME_DEFINITIONS;
        VolumeGroups.UUID.table = VOLUME_GROUPS;
        VolumeGroups.RESOURCE_GROUP_NAME.table = VOLUME_GROUPS;
        VolumeGroups.VLM_NR.table = VOLUME_GROUPS;
        VolumeGroups.FLAGS.table = VOLUME_GROUPS;
    }

    public static class ColumnImpl implements Column
    {
        private final String name;
        private final int sqlType;
        private final boolean isPk;
        private final boolean isNullable;
        private @Nullable DatabaseTable table;

        public ColumnImpl(
            final String nameRef,
            final int sqlTypeRef,
            final boolean isPkRef,
            final boolean isNullableRef
        )
        {
            name = nameRef;
            sqlType = sqlTypeRef;
            isPk = isPkRef;
            isNullable = isNullableRef;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public int getSqlType()
        {
            return sqlType;
        }

        @Override
        public boolean isPk()
        {
            return isPk;
        }

        @Override
        public boolean isNullable()
        {
            return isNullable;
        }

        @Override
        public @Nullable DatabaseTable getTable()
        {
            return table;
        }

        @Override
        public String toString()
        {
            return (table == null ? "No table set" : table) + ", Column: " + name;
        }
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    public static DatabaseTable getByValue(String value)
    {
        switch (value.toUpperCase())
        {
            case "EBS_REMOTES":
                return EBS_REMOTES;
            case "FILES":
                return FILES;
            case "KEY_VALUE_STORE":
                return KEY_VALUE_STORE;
            case "LAYER_BCACHE_VOLUMES":
                return LAYER_BCACHE_VOLUMES;
            case "LAYER_CACHE_VOLUMES":
                return LAYER_CACHE_VOLUMES;
            case "LAYER_DRBD_RESOURCES":
                return LAYER_DRBD_RESOURCES;
            case "LAYER_DRBD_RESOURCE_DEFINITIONS":
                return LAYER_DRBD_RESOURCE_DEFINITIONS;
            case "LAYER_DRBD_VOLUMES":
                return LAYER_DRBD_VOLUMES;
            case "LAYER_DRBD_VOLUME_DEFINITIONS":
                return LAYER_DRBD_VOLUME_DEFINITIONS;
            case "LAYER_LUKS_VOLUMES":
                return LAYER_LUKS_VOLUMES;
            case "LAYER_RESOURCE_IDS":
                return LAYER_RESOURCE_IDS;
            case "LAYER_STORAGE_VOLUMES":
                return LAYER_STORAGE_VOLUMES;
            case "LAYER_WRITECACHE_VOLUMES":
                return LAYER_WRITECACHE_VOLUMES;
            case "LINSTOR_REMOTES":
                return LINSTOR_REMOTES;
            case "NODES":
                return NODES;
            case "NODE_CONNECTIONS":
                return NODE_CONNECTIONS;
            case "NODE_NET_INTERFACES":
                return NODE_NET_INTERFACES;
            case "NODE_STOR_POOL":
                return NODE_STOR_POOL;
            case "PROPS_CONTAINERS":
                return PROPS_CONTAINERS;
            case "RESOURCES":
                return RESOURCES;
            case "RESOURCE_CONNECTIONS":
                return RESOURCE_CONNECTIONS;
            case "RESOURCE_DEFINITIONS":
                return RESOURCE_DEFINITIONS;
            case "RESOURCE_GROUPS":
                return RESOURCE_GROUPS;
            case "S3_REMOTES":
                return S3_REMOTES;
            case "SATELLITES_CAPACITY":
                return SATELLITES_CAPACITY;
            case "SCHEDULES":
                return SCHEDULES;
            case "SEC_ACCESS_TYPES":
                return SEC_ACCESS_TYPES;
            case "SEC_ACL_MAP":
                return SEC_ACL_MAP;
            case "SEC_CONFIGURATION":
                return SEC_CONFIGURATION;
            case "SEC_DFLT_ROLES":
                return SEC_DFLT_ROLES;
            case "SEC_IDENTITIES":
                return SEC_IDENTITIES;
            case "SEC_ID_ROLE_MAP":
                return SEC_ID_ROLE_MAP;
            case "SEC_OBJECT_PROTECTION":
                return SEC_OBJECT_PROTECTION;
            case "SEC_ROLES":
                return SEC_ROLES;
            case "SEC_TYPES":
                return SEC_TYPES;
            case "SEC_TYPE_RULES":
                return SEC_TYPE_RULES;
            case "SPACE_HISTORY":
                return SPACE_HISTORY;
            case "STOR_POOL_DEFINITIONS":
                return STOR_POOL_DEFINITIONS;
            case "TRACKING_DATE":
                return TRACKING_DATE;
            case "VOLUMES":
                return VOLUMES;
            case "VOLUME_CONNECTIONS":
                return VOLUME_CONNECTIONS;
            case "VOLUME_DEFINITIONS":
                return VOLUME_DEFINITIONS;
            case "VOLUME_GROUPS":
                return VOLUME_GROUPS;
            default:
                throw new ImplementationError("Unknown database table: " + value);
        }
    }
}
