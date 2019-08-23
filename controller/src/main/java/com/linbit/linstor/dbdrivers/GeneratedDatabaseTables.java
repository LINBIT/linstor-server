package com.linbit.linstor.dbdrivers;

import java.sql.Types;

public class GeneratedDatabaseTables
{
    /**
     * Marker interface. All following tables share this interface
     */
    public interface Table
    {
        /**
         * Returns all columns of the current table
         */
        Column[] values();

        /**
         * Returns the name of the current table
         */
        String name();
    }

    private GeneratedDatabaseTables()
    {
    }

    // Schema name
    public static final String DATABASE_SCHEMA_NAME = "LINSTOR";

    private static final GeneratedDatabaseTables INSTANCE = new GeneratedDatabaseTables();

    public static final KeyValueStore KEY_VALUE_STORE = INSTANCE.new KeyValueStore();
    public static final LayerDrbdResources LAYER_DRBD_RESOURCES = INSTANCE.new LayerDrbdResources();
    public static final LayerDrbdResourceDefinitions LAYER_DRBD_RESOURCE_DEFINITIONS = INSTANCE.new LayerDrbdResourceDefinitions();
    public static final LayerDrbdVolumes LAYER_DRBD_VOLUMES = INSTANCE.new LayerDrbdVolumes();
    public static final LayerDrbdVolumeDefinitions LAYER_DRBD_VOLUME_DEFINITIONS = INSTANCE.new LayerDrbdVolumeDefinitions();
    public static final LayerLuksVolumes LAYER_LUKS_VOLUMES = INSTANCE.new LayerLuksVolumes();
    public static final LayerResourceIds LAYER_RESOURCE_IDS = INSTANCE.new LayerResourceIds();
    public static final LayerStorageVolumes LAYER_STORAGE_VOLUMES = INSTANCE.new LayerStorageVolumes();
    public static final LayerSwordfishVolumeDefinitions LAYER_SWORDFISH_VOLUME_DEFINITIONS = INSTANCE.new LayerSwordfishVolumeDefinitions();
    public static final Nodes NODES = INSTANCE.new Nodes();
    public static final NodeConnections NODE_CONNECTIONS = INSTANCE.new NodeConnections();
    public static final NodeNetInterfaces NODE_NET_INTERFACES = INSTANCE.new NodeNetInterfaces();
    public static final NodeStorPool NODE_STOR_POOL = INSTANCE.new NodeStorPool();
    public static final PropsContainers PROPS_CONTAINERS = INSTANCE.new PropsContainers();
    public static final Resources RESOURCES = INSTANCE.new Resources();
    public static final ResourceConnections RESOURCE_CONNECTIONS = INSTANCE.new ResourceConnections();
    public static final ResourceDefinitions RESOURCE_DEFINITIONS = INSTANCE.new ResourceDefinitions();
    public static final ResourceGroups RESOURCE_GROUPS = INSTANCE.new ResourceGroups();
    public static final SecAccessTypes SEC_ACCESS_TYPES = INSTANCE.new SecAccessTypes();
    public static final SecAclMap SEC_ACL_MAP = INSTANCE.new SecAclMap();
    public static final SecConfiguration SEC_CONFIGURATION = INSTANCE.new SecConfiguration();
    public static final SecDfltRoles SEC_DFLT_ROLES = INSTANCE.new SecDfltRoles();
    public static final SecIdentities SEC_IDENTITIES = INSTANCE.new SecIdentities();
    public static final SecIdRoleMap SEC_ID_ROLE_MAP = INSTANCE.new SecIdRoleMap();
    public static final SecObjectProtection SEC_OBJECT_PROTECTION = INSTANCE.new SecObjectProtection();
    public static final SecRoles SEC_ROLES = INSTANCE.new SecRoles();
    public static final SecTypes SEC_TYPES = INSTANCE.new SecTypes();
    public static final SecTypeRules SEC_TYPE_RULES = INSTANCE.new SecTypeRules();
    public static final Snapshots SNAPSHOTS = INSTANCE.new Snapshots();
    public static final SnapshotDefinitions SNAPSHOT_DEFINITIONS = INSTANCE.new SnapshotDefinitions();
    public static final SnapshotVolumes SNAPSHOT_VOLUMES = INSTANCE.new SnapshotVolumes();
    public static final SnapshotVolumeDefinitions SNAPSHOT_VOLUME_DEFINITIONS = INSTANCE.new SnapshotVolumeDefinitions();
    public static final StorPoolDefinitions STOR_POOL_DEFINITIONS = INSTANCE.new StorPoolDefinitions();
    public static final Volumes VOLUMES = INSTANCE.new Volumes();
    public static final VolumeConnections VOLUME_CONNECTIONS = INSTANCE.new VolumeConnections();
    public static final VolumeDefinitions VOLUME_DEFINITIONS = INSTANCE.new VolumeDefinitions();
    public static final VolumeGroups VOLUME_GROUPS = INSTANCE.new VolumeGroups();

    public class KeyValueStore implements Table
    {
        private KeyValueStore() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column KVS_NAME = new Column(1, "KVS_NAME", Types.VARCHAR, true, false);
        public final Column KVS_DSP_NAME = new Column(2, "KVS_DSP_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "KEY_VALUE_STORE";
        }
    }

    public class LayerDrbdResources implements Table
    {
        private LayerDrbdResources() { }

        public final Column LAYER_RESOURCE_ID = new Column(0, "LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public final Column PEER_SLOTS = new Column(1, "PEER_SLOTS", Types.INTEGER, false, false);
        public final Column AL_STRIPES = new Column(2, "AL_STRIPES", Types.INTEGER, false, false);
        public final Column AL_STRIPE_SIZE = new Column(3, "AL_STRIPE_SIZE", Types.BIGINT, false, false);
        public final Column FLAGS = new Column(4, "FLAGS", Types.BIGINT, false, false);
        public final Column NODE_ID = new Column(5, "NODE_ID", Types.INTEGER, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "LAYER_DRBD_RESOURCES";
        }
    }

    public class LayerDrbdResourceDefinitions implements Table
    {
        private LayerDrbdResourceDefinitions() { }

        public final Column RESOURCE_NAME = new Column(0, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME_SUFFIX = new Column(1, "RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public final Column PEER_SLOTS = new Column(2, "PEER_SLOTS", Types.INTEGER, false, false);
        public final Column AL_STRIPES = new Column(3, "AL_STRIPES", Types.INTEGER, false, false);
        public final Column AL_STRIPE_SIZE = new Column(4, "AL_STRIPE_SIZE", Types.BIGINT, false, false);
        public final Column TCP_PORT = new Column(5, "TCP_PORT", Types.INTEGER, false, false);
        public final Column TRANSPORT_TYPE = new Column(6, "TRANSPORT_TYPE", Types.VARCHAR, false, false);
        public final Column SECRET = new Column(7, "SECRET", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
        {
            RESOURCE_NAME,
            RESOURCE_NAME_SUFFIX,
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
        public String name()
        {
            return "LAYER_DRBD_RESOURCE_DEFINITIONS";
        }
    }

    public class LayerDrbdVolumes implements Table
    {
        private LayerDrbdVolumes() { }

        public final Column LAYER_RESOURCE_ID = new Column(0, "LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public final Column VLM_NR = new Column(1, "VLM_NR", Types.INTEGER, true, false);
        public final Column NODE_NAME = new Column(2, "NODE_NAME", Types.VARCHAR, false, true);
        public final Column POOL_NAME = new Column(3, "POOL_NAME", Types.VARCHAR, false, true);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "LAYER_DRBD_VOLUMES";
        }
    }

    public class LayerDrbdVolumeDefinitions implements Table
    {
        private LayerDrbdVolumeDefinitions() { }

        public final Column RESOURCE_NAME = new Column(0, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME_SUFFIX = new Column(1, "RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(2, "VLM_NR", Types.INTEGER, true, false);
        public final Column VLM_MINOR_NR = new Column(3, "VLM_MINOR_NR", Types.INTEGER, false, false);

        public final Column[] ALL = new Column[]
        {
            RESOURCE_NAME,
            RESOURCE_NAME_SUFFIX,
            VLM_NR,
            VLM_MINOR_NR
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "LAYER_DRBD_VOLUME_DEFINITIONS";
        }
    }

    public class LayerLuksVolumes implements Table
    {
        private LayerLuksVolumes() { }

        public final Column LAYER_RESOURCE_ID = new Column(0, "LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public final Column VLM_NR = new Column(1, "VLM_NR", Types.INTEGER, true, false);
        public final Column ENCRYPTED_PASSWORD = new Column(2, "ENCRYPTED_PASSWORD", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "LAYER_LUKS_VOLUMES";
        }
    }

    public class LayerResourceIds implements Table
    {
        private LayerResourceIds() { }

        public final Column LAYER_RESOURCE_ID = new Column(0, "LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, false, false);
        public final Column RESOURCE_NAME = new Column(2, "RESOURCE_NAME", Types.VARCHAR, false, false);
        public final Column LAYER_RESOURCE_KIND = new Column(3, "LAYER_RESOURCE_KIND", Types.VARCHAR, false, false);
        public final Column LAYER_RESOURCE_PARENT_ID = new Column(4, "LAYER_RESOURCE_PARENT_ID", Types.INTEGER, false, true);
        public final Column LAYER_RESOURCE_SUFFIX = new Column(5, "LAYER_RESOURCE_SUFFIX", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
        {
            LAYER_RESOURCE_ID,
            NODE_NAME,
            RESOURCE_NAME,
            LAYER_RESOURCE_KIND,
            LAYER_RESOURCE_PARENT_ID,
            LAYER_RESOURCE_SUFFIX
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "LAYER_RESOURCE_IDS";
        }
    }

    public class LayerStorageVolumes implements Table
    {
        private LayerStorageVolumes() { }

        public final Column LAYER_RESOURCE_ID = new Column(0, "LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public final Column VLM_NR = new Column(1, "VLM_NR", Types.INTEGER, true, false);
        public final Column PROVIDER_KIND = new Column(2, "PROVIDER_KIND", Types.VARCHAR, false, false);
        public final Column NODE_NAME = new Column(3, "NODE_NAME", Types.VARCHAR, false, false);
        public final Column STOR_POOL_NAME = new Column(4, "STOR_POOL_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "LAYER_STORAGE_VOLUMES";
        }
    }

    public class LayerSwordfishVolumeDefinitions implements Table
    {
        private LayerSwordfishVolumeDefinitions() { }

        public final Column RESOURCE_NAME = new Column(0, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME_SUFFIX = new Column(1, "RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(2, "VLM_NR", Types.INTEGER, true, false);
        public final Column VLM_ODATA = new Column(3, "VLM_ODATA", Types.VARCHAR, false, true);

        public final Column[] ALL = new Column[]
        {
            RESOURCE_NAME,
            RESOURCE_NAME_SUFFIX,
            VLM_NR,
            VLM_ODATA
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "LAYER_SWORDFISH_VOLUME_DEFINITIONS";
        }
    }

    public class Nodes implements Table
    {
        private Nodes() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, true, false);
        public final Column NODE_DSP_NAME = new Column(2, "NODE_DSP_NAME", Types.VARCHAR, false, false);
        public final Column NODE_FLAGS = new Column(3, "NODE_FLAGS", Types.BIGINT, false, false);
        public final Column NODE_TYPE = new Column(4, "NODE_TYPE", Types.INTEGER, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "NODES";
        }
    }

    public class NodeConnections implements Table
    {
        private NodeConnections() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME_SRC = new Column(1, "NODE_NAME_SRC", Types.VARCHAR, true, false);
        public final Column NODE_NAME_DST = new Column(2, "NODE_NAME_DST", Types.VARCHAR, true, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "NODE_CONNECTIONS";
        }
    }

    public class NodeNetInterfaces implements Table
    {
        private NodeNetInterfaces() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, true, false);
        public final Column NODE_NET_NAME = new Column(2, "NODE_NET_NAME", Types.VARCHAR, true, false);
        public final Column NODE_NET_DSP_NAME = new Column(3, "NODE_NET_DSP_NAME", Types.VARCHAR, false, false);
        public final Column INET_ADDRESS = new Column(4, "INET_ADDRESS", Types.VARCHAR, false, false);
        public final Column STLT_CONN_PORT = new Column(5, "STLT_CONN_PORT", Types.SMALLINT, false, true);
        public final Column STLT_CONN_ENCR_TYPE = new Column(6, "STLT_CONN_ENCR_TYPE", Types.VARCHAR, false, true);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "NODE_NET_INTERFACES";
        }
    }

    public class NodeStorPool implements Table
    {
        private NodeStorPool() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, true, false);
        public final Column POOL_NAME = new Column(2, "POOL_NAME", Types.VARCHAR, true, false);
        public final Column DRIVER_NAME = new Column(3, "DRIVER_NAME", Types.VARCHAR, false, false);
        public final Column FREE_SPACE_MGR_NAME = new Column(4, "FREE_SPACE_MGR_NAME", Types.VARCHAR, false, false);
        public final Column FREE_SPACE_MGR_DSP_NAME = new Column(5, "FREE_SPACE_MGR_DSP_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            POOL_NAME,
            DRIVER_NAME,
            FREE_SPACE_MGR_NAME,
            FREE_SPACE_MGR_DSP_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "NODE_STOR_POOL";
        }
    }

    public class PropsContainers implements Table
    {
        private PropsContainers() { }

        public final Column PROPS_INSTANCE = new Column(0, "PROPS_INSTANCE", Types.VARCHAR, true, false);
        public final Column PROP_KEY = new Column(1, "PROP_KEY", Types.VARCHAR, true, false);
        public final Column PROP_VALUE = new Column(2, "PROP_VALUE", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "PROPS_CONTAINERS";
        }
    }

    public class Resources implements Table
    {
        private Resources() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME = new Column(2, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_FLAGS = new Column(3, "RESOURCE_FLAGS", Types.BIGINT, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            RESOURCE_NAME,
            RESOURCE_FLAGS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "RESOURCES";
        }
    }

    public class ResourceConnections implements Table
    {
        private ResourceConnections() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME_SRC = new Column(1, "NODE_NAME_SRC", Types.VARCHAR, true, false);
        public final Column NODE_NAME_DST = new Column(2, "NODE_NAME_DST", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME = new Column(3, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column FLAGS = new Column(4, "FLAGS", Types.BIGINT, false, false);
        public final Column TCP_PORT = new Column(5, "TCP_PORT", Types.INTEGER, false, true);

        public final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME_SRC,
            NODE_NAME_DST,
            RESOURCE_NAME,
            FLAGS,
            TCP_PORT
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "RESOURCE_CONNECTIONS";
        }
    }

    public class ResourceDefinitions implements Table
    {
        private ResourceDefinitions() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column RESOURCE_NAME = new Column(1, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_DSP_NAME = new Column(2, "RESOURCE_DSP_NAME", Types.VARCHAR, false, false);
        public final Column RESOURCE_FLAGS = new Column(3, "RESOURCE_FLAGS", Types.BIGINT, false, false);
        public final Column LAYER_STACK = new Column(4, "LAYER_STACK", Types.VARCHAR, false, false);
        public final Column RESOURCE_EXTERNAL_NAME = new Column(5, "RESOURCE_EXTERNAL_NAME", Types.BLOB, false, true);
        public final Column RESOURCE_GROUP_NAME = new Column(6, "RESOURCE_GROUP_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_NAME,
            RESOURCE_DSP_NAME,
            RESOURCE_FLAGS,
            LAYER_STACK,
            RESOURCE_EXTERNAL_NAME,
            RESOURCE_GROUP_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "RESOURCE_DEFINITIONS";
        }
    }

    public class ResourceGroups implements Table
    {
        private ResourceGroups() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column RESOURCE_GROUP_NAME = new Column(1, "RESOURCE_GROUP_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_GROUP_DSP_NAME = new Column(2, "RESOURCE_GROUP_DSP_NAME", Types.VARCHAR, false, false);
        public final Column DESCRIPTION = new Column(3, "DESCRIPTION", Types.VARCHAR, false, true);
        public final Column LAYER_STACK = new Column(4, "LAYER_STACK", Types.VARCHAR, false, true);
        public final Column REPLICA_COUNT = new Column(5, "REPLICA_COUNT", Types.INTEGER, false, true);
        public final Column POOL_NAME = new Column(6, "POOL_NAME", Types.VARCHAR, false, true);
        public final Column DO_NOT_PLACE_WITH_RSC_REGEX = new Column(7, "DO_NOT_PLACE_WITH_RSC_REGEX", Types.VARCHAR, false, true);
        public final Column DO_NOT_PLACE_WITH_RSC_LIST = new Column(8, "DO_NOT_PLACE_WITH_RSC_LIST", Types.VARCHAR, false, true);
        public final Column REPLICAS_ON_SAME = new Column(9, "REPLICAS_ON_SAME", Types.BLOB, false, true);
        public final Column REPLICAS_ON_DIFFERENT = new Column(10, "REPLICAS_ON_DIFFERENT", Types.BLOB, false, true);
        public final Column ALLOWED_PROVIDER_LIST = new Column(11, "ALLOWED_PROVIDER_LIST", Types.VARCHAR, false, true);
        public final Column DISKLESS_ON_REMAINING = new Column(12, "DISKLESS_ON_REMAINING", Types.BOOLEAN, false, true);

        public final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_GROUP_NAME,
            RESOURCE_GROUP_DSP_NAME,
            DESCRIPTION,
            LAYER_STACK,
            REPLICA_COUNT,
            POOL_NAME,
            DO_NOT_PLACE_WITH_RSC_REGEX,
            DO_NOT_PLACE_WITH_RSC_LIST,
            REPLICAS_ON_SAME,
            REPLICAS_ON_DIFFERENT,
            ALLOWED_PROVIDER_LIST,
            DISKLESS_ON_REMAINING
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "RESOURCE_GROUPS";
        }
    }

    public class SecAccessTypes implements Table
    {
        private SecAccessTypes() { }

        public final Column ACCESS_TYPE_NAME = new Column(0, "ACCESS_TYPE_NAME", Types.VARCHAR, true, false);
        public final Column ACCESS_TYPE_VALUE = new Column(1, "ACCESS_TYPE_VALUE", Types.SMALLINT, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_ACCESS_TYPES";
        }
    }

    public class SecAclMap implements Table
    {
        private SecAclMap() { }

        public final Column OBJECT_PATH = new Column(0, "OBJECT_PATH", Types.VARCHAR, true, false);
        public final Column ROLE_NAME = new Column(1, "ROLE_NAME", Types.VARCHAR, true, false);
        public final Column ACCESS_TYPE = new Column(2, "ACCESS_TYPE", Types.SMALLINT, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_ACL_MAP";
        }
    }

    public class SecConfiguration implements Table
    {
        private SecConfiguration() { }

        public final Column ENTRY_KEY = new Column(0, "ENTRY_KEY", Types.VARCHAR, true, false);
        public final Column ENTRY_DSP_KEY = new Column(1, "ENTRY_DSP_KEY", Types.VARCHAR, false, false);
        public final Column ENTRY_VALUE = new Column(2, "ENTRY_VALUE", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_CONFIGURATION";
        }
    }

    public class SecDfltRoles implements Table
    {
        private SecDfltRoles() { }

        public final Column IDENTITY_NAME = new Column(0, "IDENTITY_NAME", Types.VARCHAR, true, false);
        public final Column ROLE_NAME = new Column(1, "ROLE_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_DFLT_ROLES";
        }
    }

    public class SecIdentities implements Table
    {
        private SecIdentities() { }

        public final Column IDENTITY_NAME = new Column(0, "IDENTITY_NAME", Types.VARCHAR, true, false);
        public final Column IDENTITY_DSP_NAME = new Column(1, "IDENTITY_DSP_NAME", Types.VARCHAR, false, false);
        public final Column PASS_SALT = new Column(2, "PASS_SALT", Types.CHAR, false, true);
        public final Column PASS_HASH = new Column(3, "PASS_HASH", Types.CHAR, false, true);
        public final Column ID_ENABLED = new Column(4, "ID_ENABLED", Types.BOOLEAN, false, false);
        public final Column ID_LOCKED = new Column(5, "ID_LOCKED", Types.BOOLEAN, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_IDENTITIES";
        }
    }

    public class SecIdRoleMap implements Table
    {
        private SecIdRoleMap() { }

        public final Column IDENTITY_NAME = new Column(0, "IDENTITY_NAME", Types.VARCHAR, true, false);
        public final Column ROLE_NAME = new Column(1, "ROLE_NAME", Types.VARCHAR, true, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_ID_ROLE_MAP";
        }
    }

    public class SecObjectProtection implements Table
    {
        private SecObjectProtection() { }

        public final Column OBJECT_PATH = new Column(0, "OBJECT_PATH", Types.VARCHAR, true, false);
        public final Column CREATOR_IDENTITY_NAME = new Column(1, "CREATOR_IDENTITY_NAME", Types.VARCHAR, false, false);
        public final Column OWNER_ROLE_NAME = new Column(2, "OWNER_ROLE_NAME", Types.VARCHAR, false, false);
        public final Column SECURITY_TYPE_NAME = new Column(3, "SECURITY_TYPE_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_OBJECT_PROTECTION";
        }
    }

    public class SecRoles implements Table
    {
        private SecRoles() { }

        public final Column ROLE_NAME = new Column(0, "ROLE_NAME", Types.VARCHAR, true, false);
        public final Column ROLE_DSP_NAME = new Column(1, "ROLE_DSP_NAME", Types.VARCHAR, false, false);
        public final Column DOMAIN_NAME = new Column(2, "DOMAIN_NAME", Types.VARCHAR, false, false);
        public final Column ROLE_ENABLED = new Column(3, "ROLE_ENABLED", Types.BOOLEAN, false, false);
        public final Column ROLE_PRIVILEGES = new Column(4, "ROLE_PRIVILEGES", Types.BIGINT, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_ROLES";
        }
    }

    public class SecTypes implements Table
    {
        private SecTypes() { }

        public final Column TYPE_NAME = new Column(0, "TYPE_NAME", Types.VARCHAR, true, false);
        public final Column TYPE_DSP_NAME = new Column(1, "TYPE_DSP_NAME", Types.VARCHAR, false, false);
        public final Column TYPE_ENABLED = new Column(2, "TYPE_ENABLED", Types.BOOLEAN, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_TYPES";
        }
    }

    public class SecTypeRules implements Table
    {
        private SecTypeRules() { }

        public final Column DOMAIN_NAME = new Column(0, "DOMAIN_NAME", Types.VARCHAR, true, false);
        public final Column TYPE_NAME = new Column(1, "TYPE_NAME", Types.VARCHAR, true, false);
        public final Column ACCESS_TYPE = new Column(2, "ACCESS_TYPE", Types.SMALLINT, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "SEC_TYPE_RULES";
        }
    }

    public class Snapshots implements Table
    {
        private Snapshots() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME = new Column(2, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column SNAPSHOT_NAME = new Column(3, "SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public final Column SNAPSHOT_FLAGS = new Column(4, "SNAPSHOT_FLAGS", Types.BIGINT, false, false);
        public final Column NODE_ID = new Column(5, "NODE_ID", Types.INTEGER, false, true);
        public final Column LAYER_STACK = new Column(6, "LAYER_STACK", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            SNAPSHOT_FLAGS,
            NODE_ID,
            LAYER_STACK
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "SNAPSHOTS";
        }
    }

    public class SnapshotDefinitions implements Table
    {
        private SnapshotDefinitions() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column RESOURCE_NAME = new Column(1, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column SNAPSHOT_NAME = new Column(2, "SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public final Column SNAPSHOT_DSP_NAME = new Column(3, "SNAPSHOT_DSP_NAME", Types.VARCHAR, false, false);
        public final Column SNAPSHOT_FLAGS = new Column(4, "SNAPSHOT_FLAGS", Types.BIGINT, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            SNAPSHOT_DSP_NAME,
            SNAPSHOT_FLAGS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "SNAPSHOT_DEFINITIONS";
        }
    }

    public class SnapshotVolumes implements Table
    {
        private SnapshotVolumes() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME = new Column(2, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column SNAPSHOT_NAME = new Column(3, "SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(4, "VLM_NR", Types.INTEGER, true, false);
        public final Column STOR_POOL_NAME = new Column(5, "STOR_POOL_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            VLM_NR,
            STOR_POOL_NAME
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "SNAPSHOT_VOLUMES";
        }
    }

    public class SnapshotVolumeDefinitions implements Table
    {
        private SnapshotVolumeDefinitions() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column RESOURCE_NAME = new Column(1, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column SNAPSHOT_NAME = new Column(2, "SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(3, "VLM_NR", Types.INTEGER, true, false);
        public final Column VLM_SIZE = new Column(4, "VLM_SIZE", Types.BIGINT, false, false);
        public final Column SNAPSHOT_FLAGS = new Column(5, "SNAPSHOT_FLAGS", Types.BIGINT, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            VLM_NR,
            VLM_SIZE,
            SNAPSHOT_FLAGS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "SNAPSHOT_VOLUME_DEFINITIONS";
        }
    }

    public class StorPoolDefinitions implements Table
    {
        private StorPoolDefinitions() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column POOL_NAME = new Column(1, "POOL_NAME", Types.VARCHAR, true, false);
        public final Column POOL_DSP_NAME = new Column(2, "POOL_DSP_NAME", Types.VARCHAR, false, false);

        public final Column[] ALL = new Column[]
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
        public String name()
        {
            return "STOR_POOL_DEFINITIONS";
        }
    }

    public class Volumes implements Table
    {
        private Volumes() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME = new Column(1, "NODE_NAME", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME = new Column(2, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(3, "VLM_NR", Types.INTEGER, true, false);
        public final Column VLM_FLAGS = new Column(4, "VLM_FLAGS", Types.BIGINT, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            RESOURCE_NAME,
            VLM_NR,
            VLM_FLAGS
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "VOLUMES";
        }
    }

    public class VolumeConnections implements Table
    {
        private VolumeConnections() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column NODE_NAME_SRC = new Column(1, "NODE_NAME_SRC", Types.VARCHAR, true, false);
        public final Column NODE_NAME_DST = new Column(2, "NODE_NAME_DST", Types.VARCHAR, true, false);
        public final Column RESOURCE_NAME = new Column(3, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(4, "VLM_NR", Types.INTEGER, true, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME_SRC,
            NODE_NAME_DST,
            RESOURCE_NAME,
            VLM_NR
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "VOLUME_CONNECTIONS";
        }
    }

    public class VolumeDefinitions implements Table
    {
        private VolumeDefinitions() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column RESOURCE_NAME = new Column(1, "RESOURCE_NAME", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(2, "VLM_NR", Types.INTEGER, true, false);
        public final Column VLM_SIZE = new Column(3, "VLM_SIZE", Types.BIGINT, false, false);
        public final Column VLM_FLAGS = new Column(4, "VLM_FLAGS", Types.BIGINT, false, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_NAME,
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
        public String name()
        {
            return "VOLUME_DEFINITIONS";
        }
    }

    public class VolumeGroups implements Table
    {
        private VolumeGroups() { }

        public final Column UUID = new Column(0, "UUID", Types.CHAR, false, false);
        public final Column RESOURCE_GROUP_NAME = new Column(1, "RESOURCE_GROUP_NAME", Types.VARCHAR, true, false);
        public final Column VLM_NR = new Column(2, "VLM_NR", Types.INTEGER, true, false);

        public final Column[] ALL = new Column[]
        {
            UUID,
            RESOURCE_GROUP_NAME,
            VLM_NR
        };

        @Override
        public Column[] values()
        {
            return ALL;
        }

        @Override
        public String name()
        {
            return "VOLUME_GROUPS";
        }
    }


    public static class Column
    {
        public final int index;
        public final String name;
        public final int sqlType;
        public final boolean isPk;
        public final boolean isNullable;
        private Table table;

        public Column(
            final int indexRef,
            final String nameRef,
            final int sqlTypeRef,
            final boolean isPkRef,
            final boolean isNullableRef
        )
        {
            index = indexRef;
            name = nameRef;
            sqlType = sqlTypeRef;
            isPk = isPkRef;
            isNullable = isNullableRef;
        }

        private void setTable(Table tableRef)
        {
            table = tableRef;
        }

        public Table getTable()
        {
            return table;
        }
    }

    static
    {
        KEY_VALUE_STORE.UUID.setTable(KEY_VALUE_STORE);
        KEY_VALUE_STORE.KVS_NAME.setTable(KEY_VALUE_STORE);
        KEY_VALUE_STORE.KVS_DSP_NAME.setTable(KEY_VALUE_STORE);
        LAYER_DRBD_RESOURCES.LAYER_RESOURCE_ID.setTable(LAYER_DRBD_RESOURCES);
        LAYER_DRBD_RESOURCES.PEER_SLOTS.setTable(LAYER_DRBD_RESOURCES);
        LAYER_DRBD_RESOURCES.AL_STRIPES.setTable(LAYER_DRBD_RESOURCES);
        LAYER_DRBD_RESOURCES.AL_STRIPE_SIZE.setTable(LAYER_DRBD_RESOURCES);
        LAYER_DRBD_RESOURCES.FLAGS.setTable(LAYER_DRBD_RESOURCES);
        LAYER_DRBD_RESOURCES.NODE_ID.setTable(LAYER_DRBD_RESOURCES);
        LAYER_DRBD_RESOURCE_DEFINITIONS.RESOURCE_NAME.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_RESOURCE_DEFINITIONS.RESOURCE_NAME_SUFFIX.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_RESOURCE_DEFINITIONS.PEER_SLOTS.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_RESOURCE_DEFINITIONS.AL_STRIPES.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_RESOURCE_DEFINITIONS.AL_STRIPE_SIZE.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_RESOURCE_DEFINITIONS.TCP_PORT.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_RESOURCE_DEFINITIONS.TRANSPORT_TYPE.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_RESOURCE_DEFINITIONS.SECRET.setTable(LAYER_DRBD_RESOURCE_DEFINITIONS);
        LAYER_DRBD_VOLUMES.LAYER_RESOURCE_ID.setTable(LAYER_DRBD_VOLUMES);
        LAYER_DRBD_VOLUMES.VLM_NR.setTable(LAYER_DRBD_VOLUMES);
        LAYER_DRBD_VOLUMES.NODE_NAME.setTable(LAYER_DRBD_VOLUMES);
        LAYER_DRBD_VOLUMES.POOL_NAME.setTable(LAYER_DRBD_VOLUMES);
        LAYER_DRBD_VOLUME_DEFINITIONS.RESOURCE_NAME.setTable(LAYER_DRBD_VOLUME_DEFINITIONS);
        LAYER_DRBD_VOLUME_DEFINITIONS.RESOURCE_NAME_SUFFIX.setTable(LAYER_DRBD_VOLUME_DEFINITIONS);
        LAYER_DRBD_VOLUME_DEFINITIONS.VLM_NR.setTable(LAYER_DRBD_VOLUME_DEFINITIONS);
        LAYER_DRBD_VOLUME_DEFINITIONS.VLM_MINOR_NR.setTable(LAYER_DRBD_VOLUME_DEFINITIONS);
        LAYER_LUKS_VOLUMES.LAYER_RESOURCE_ID.setTable(LAYER_LUKS_VOLUMES);
        LAYER_LUKS_VOLUMES.VLM_NR.setTable(LAYER_LUKS_VOLUMES);
        LAYER_LUKS_VOLUMES.ENCRYPTED_PASSWORD.setTable(LAYER_LUKS_VOLUMES);
        LAYER_RESOURCE_IDS.LAYER_RESOURCE_ID.setTable(LAYER_RESOURCE_IDS);
        LAYER_RESOURCE_IDS.NODE_NAME.setTable(LAYER_RESOURCE_IDS);
        LAYER_RESOURCE_IDS.RESOURCE_NAME.setTable(LAYER_RESOURCE_IDS);
        LAYER_RESOURCE_IDS.LAYER_RESOURCE_KIND.setTable(LAYER_RESOURCE_IDS);
        LAYER_RESOURCE_IDS.LAYER_RESOURCE_PARENT_ID.setTable(LAYER_RESOURCE_IDS);
        LAYER_RESOURCE_IDS.LAYER_RESOURCE_SUFFIX.setTable(LAYER_RESOURCE_IDS);
        LAYER_STORAGE_VOLUMES.LAYER_RESOURCE_ID.setTable(LAYER_STORAGE_VOLUMES);
        LAYER_STORAGE_VOLUMES.VLM_NR.setTable(LAYER_STORAGE_VOLUMES);
        LAYER_STORAGE_VOLUMES.PROVIDER_KIND.setTable(LAYER_STORAGE_VOLUMES);
        LAYER_STORAGE_VOLUMES.NODE_NAME.setTable(LAYER_STORAGE_VOLUMES);
        LAYER_STORAGE_VOLUMES.STOR_POOL_NAME.setTable(LAYER_STORAGE_VOLUMES);
        LAYER_SWORDFISH_VOLUME_DEFINITIONS.RESOURCE_NAME.setTable(LAYER_SWORDFISH_VOLUME_DEFINITIONS);
        LAYER_SWORDFISH_VOLUME_DEFINITIONS.RESOURCE_NAME_SUFFIX.setTable(LAYER_SWORDFISH_VOLUME_DEFINITIONS);
        LAYER_SWORDFISH_VOLUME_DEFINITIONS.VLM_NR.setTable(LAYER_SWORDFISH_VOLUME_DEFINITIONS);
        LAYER_SWORDFISH_VOLUME_DEFINITIONS.VLM_ODATA.setTable(LAYER_SWORDFISH_VOLUME_DEFINITIONS);
        NODES.UUID.setTable(NODES);
        NODES.NODE_NAME.setTable(NODES);
        NODES.NODE_DSP_NAME.setTable(NODES);
        NODES.NODE_FLAGS.setTable(NODES);
        NODES.NODE_TYPE.setTable(NODES);
        NODE_CONNECTIONS.UUID.setTable(NODE_CONNECTIONS);
        NODE_CONNECTIONS.NODE_NAME_SRC.setTable(NODE_CONNECTIONS);
        NODE_CONNECTIONS.NODE_NAME_DST.setTable(NODE_CONNECTIONS);
        NODE_NET_INTERFACES.UUID.setTable(NODE_NET_INTERFACES);
        NODE_NET_INTERFACES.NODE_NAME.setTable(NODE_NET_INTERFACES);
        NODE_NET_INTERFACES.NODE_NET_NAME.setTable(NODE_NET_INTERFACES);
        NODE_NET_INTERFACES.NODE_NET_DSP_NAME.setTable(NODE_NET_INTERFACES);
        NODE_NET_INTERFACES.INET_ADDRESS.setTable(NODE_NET_INTERFACES);
        NODE_NET_INTERFACES.STLT_CONN_PORT.setTable(NODE_NET_INTERFACES);
        NODE_NET_INTERFACES.STLT_CONN_ENCR_TYPE.setTable(NODE_NET_INTERFACES);
        NODE_STOR_POOL.UUID.setTable(NODE_STOR_POOL);
        NODE_STOR_POOL.NODE_NAME.setTable(NODE_STOR_POOL);
        NODE_STOR_POOL.POOL_NAME.setTable(NODE_STOR_POOL);
        NODE_STOR_POOL.DRIVER_NAME.setTable(NODE_STOR_POOL);
        NODE_STOR_POOL.FREE_SPACE_MGR_NAME.setTable(NODE_STOR_POOL);
        NODE_STOR_POOL.FREE_SPACE_MGR_DSP_NAME.setTable(NODE_STOR_POOL);
        PROPS_CONTAINERS.PROPS_INSTANCE.setTable(PROPS_CONTAINERS);
        PROPS_CONTAINERS.PROP_KEY.setTable(PROPS_CONTAINERS);
        PROPS_CONTAINERS.PROP_VALUE.setTable(PROPS_CONTAINERS);
        RESOURCES.UUID.setTable(RESOURCES);
        RESOURCES.NODE_NAME.setTable(RESOURCES);
        RESOURCES.RESOURCE_NAME.setTable(RESOURCES);
        RESOURCES.RESOURCE_FLAGS.setTable(RESOURCES);
        RESOURCE_CONNECTIONS.UUID.setTable(RESOURCE_CONNECTIONS);
        RESOURCE_CONNECTIONS.NODE_NAME_SRC.setTable(RESOURCE_CONNECTIONS);
        RESOURCE_CONNECTIONS.NODE_NAME_DST.setTable(RESOURCE_CONNECTIONS);
        RESOURCE_CONNECTIONS.RESOURCE_NAME.setTable(RESOURCE_CONNECTIONS);
        RESOURCE_CONNECTIONS.FLAGS.setTable(RESOURCE_CONNECTIONS);
        RESOURCE_CONNECTIONS.TCP_PORT.setTable(RESOURCE_CONNECTIONS);
        RESOURCE_DEFINITIONS.UUID.setTable(RESOURCE_DEFINITIONS);
        RESOURCE_DEFINITIONS.RESOURCE_NAME.setTable(RESOURCE_DEFINITIONS);
        RESOURCE_DEFINITIONS.RESOURCE_DSP_NAME.setTable(RESOURCE_DEFINITIONS);
        RESOURCE_DEFINITIONS.RESOURCE_FLAGS.setTable(RESOURCE_DEFINITIONS);
        RESOURCE_DEFINITIONS.LAYER_STACK.setTable(RESOURCE_DEFINITIONS);
        RESOURCE_DEFINITIONS.RESOURCE_EXTERNAL_NAME.setTable(RESOURCE_DEFINITIONS);
        RESOURCE_DEFINITIONS.RESOURCE_GROUP_NAME.setTable(RESOURCE_DEFINITIONS);
        RESOURCE_GROUPS.UUID.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.RESOURCE_GROUP_NAME.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.RESOURCE_GROUP_DSP_NAME.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.DESCRIPTION.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.LAYER_STACK.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.REPLICA_COUNT.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.POOL_NAME.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.DO_NOT_PLACE_WITH_RSC_REGEX.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.DO_NOT_PLACE_WITH_RSC_LIST.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.REPLICAS_ON_SAME.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.REPLICAS_ON_DIFFERENT.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.ALLOWED_PROVIDER_LIST.setTable(RESOURCE_GROUPS);
        RESOURCE_GROUPS.DISKLESS_ON_REMAINING.setTable(RESOURCE_GROUPS);
        SEC_ACCESS_TYPES.ACCESS_TYPE_NAME.setTable(SEC_ACCESS_TYPES);
        SEC_ACCESS_TYPES.ACCESS_TYPE_VALUE.setTable(SEC_ACCESS_TYPES);
        SEC_ACL_MAP.OBJECT_PATH.setTable(SEC_ACL_MAP);
        SEC_ACL_MAP.ROLE_NAME.setTable(SEC_ACL_MAP);
        SEC_ACL_MAP.ACCESS_TYPE.setTable(SEC_ACL_MAP);
        SEC_CONFIGURATION.ENTRY_KEY.setTable(SEC_CONFIGURATION);
        SEC_CONFIGURATION.ENTRY_DSP_KEY.setTable(SEC_CONFIGURATION);
        SEC_CONFIGURATION.ENTRY_VALUE.setTable(SEC_CONFIGURATION);
        SEC_DFLT_ROLES.IDENTITY_NAME.setTable(SEC_DFLT_ROLES);
        SEC_DFLT_ROLES.ROLE_NAME.setTable(SEC_DFLT_ROLES);
        SEC_IDENTITIES.IDENTITY_NAME.setTable(SEC_IDENTITIES);
        SEC_IDENTITIES.IDENTITY_DSP_NAME.setTable(SEC_IDENTITIES);
        SEC_IDENTITIES.PASS_SALT.setTable(SEC_IDENTITIES);
        SEC_IDENTITIES.PASS_HASH.setTable(SEC_IDENTITIES);
        SEC_IDENTITIES.ID_ENABLED.setTable(SEC_IDENTITIES);
        SEC_IDENTITIES.ID_LOCKED.setTable(SEC_IDENTITIES);
        SEC_ID_ROLE_MAP.IDENTITY_NAME.setTable(SEC_ID_ROLE_MAP);
        SEC_ID_ROLE_MAP.ROLE_NAME.setTable(SEC_ID_ROLE_MAP);
        SEC_OBJECT_PROTECTION.OBJECT_PATH.setTable(SEC_OBJECT_PROTECTION);
        SEC_OBJECT_PROTECTION.CREATOR_IDENTITY_NAME.setTable(SEC_OBJECT_PROTECTION);
        SEC_OBJECT_PROTECTION.OWNER_ROLE_NAME.setTable(SEC_OBJECT_PROTECTION);
        SEC_OBJECT_PROTECTION.SECURITY_TYPE_NAME.setTable(SEC_OBJECT_PROTECTION);
        SEC_ROLES.ROLE_NAME.setTable(SEC_ROLES);
        SEC_ROLES.ROLE_DSP_NAME.setTable(SEC_ROLES);
        SEC_ROLES.DOMAIN_NAME.setTable(SEC_ROLES);
        SEC_ROLES.ROLE_ENABLED.setTable(SEC_ROLES);
        SEC_ROLES.ROLE_PRIVILEGES.setTable(SEC_ROLES);
        SEC_TYPES.TYPE_NAME.setTable(SEC_TYPES);
        SEC_TYPES.TYPE_DSP_NAME.setTable(SEC_TYPES);
        SEC_TYPES.TYPE_ENABLED.setTable(SEC_TYPES);
        SEC_TYPE_RULES.DOMAIN_NAME.setTable(SEC_TYPE_RULES);
        SEC_TYPE_RULES.TYPE_NAME.setTable(SEC_TYPE_RULES);
        SEC_TYPE_RULES.ACCESS_TYPE.setTable(SEC_TYPE_RULES);
        SNAPSHOTS.UUID.setTable(SNAPSHOTS);
        SNAPSHOTS.NODE_NAME.setTable(SNAPSHOTS);
        SNAPSHOTS.RESOURCE_NAME.setTable(SNAPSHOTS);
        SNAPSHOTS.SNAPSHOT_NAME.setTable(SNAPSHOTS);
        SNAPSHOTS.SNAPSHOT_FLAGS.setTable(SNAPSHOTS);
        SNAPSHOTS.NODE_ID.setTable(SNAPSHOTS);
        SNAPSHOTS.LAYER_STACK.setTable(SNAPSHOTS);
        SNAPSHOT_DEFINITIONS.UUID.setTable(SNAPSHOT_DEFINITIONS);
        SNAPSHOT_DEFINITIONS.RESOURCE_NAME.setTable(SNAPSHOT_DEFINITIONS);
        SNAPSHOT_DEFINITIONS.SNAPSHOT_NAME.setTable(SNAPSHOT_DEFINITIONS);
        SNAPSHOT_DEFINITIONS.SNAPSHOT_DSP_NAME.setTable(SNAPSHOT_DEFINITIONS);
        SNAPSHOT_DEFINITIONS.SNAPSHOT_FLAGS.setTable(SNAPSHOT_DEFINITIONS);
        SNAPSHOT_VOLUMES.UUID.setTable(SNAPSHOT_VOLUMES);
        SNAPSHOT_VOLUMES.NODE_NAME.setTable(SNAPSHOT_VOLUMES);
        SNAPSHOT_VOLUMES.RESOURCE_NAME.setTable(SNAPSHOT_VOLUMES);
        SNAPSHOT_VOLUMES.SNAPSHOT_NAME.setTable(SNAPSHOT_VOLUMES);
        SNAPSHOT_VOLUMES.VLM_NR.setTable(SNAPSHOT_VOLUMES);
        SNAPSHOT_VOLUMES.STOR_POOL_NAME.setTable(SNAPSHOT_VOLUMES);
        SNAPSHOT_VOLUME_DEFINITIONS.UUID.setTable(SNAPSHOT_VOLUME_DEFINITIONS);
        SNAPSHOT_VOLUME_DEFINITIONS.RESOURCE_NAME.setTable(SNAPSHOT_VOLUME_DEFINITIONS);
        SNAPSHOT_VOLUME_DEFINITIONS.SNAPSHOT_NAME.setTable(SNAPSHOT_VOLUME_DEFINITIONS);
        SNAPSHOT_VOLUME_DEFINITIONS.VLM_NR.setTable(SNAPSHOT_VOLUME_DEFINITIONS);
        SNAPSHOT_VOLUME_DEFINITIONS.VLM_SIZE.setTable(SNAPSHOT_VOLUME_DEFINITIONS);
        SNAPSHOT_VOLUME_DEFINITIONS.SNAPSHOT_FLAGS.setTable(SNAPSHOT_VOLUME_DEFINITIONS);
        STOR_POOL_DEFINITIONS.UUID.setTable(STOR_POOL_DEFINITIONS);
        STOR_POOL_DEFINITIONS.POOL_NAME.setTable(STOR_POOL_DEFINITIONS);
        STOR_POOL_DEFINITIONS.POOL_DSP_NAME.setTable(STOR_POOL_DEFINITIONS);
        VOLUMES.UUID.setTable(VOLUMES);
        VOLUMES.NODE_NAME.setTable(VOLUMES);
        VOLUMES.RESOURCE_NAME.setTable(VOLUMES);
        VOLUMES.VLM_NR.setTable(VOLUMES);
        VOLUMES.VLM_FLAGS.setTable(VOLUMES);
        VOLUME_CONNECTIONS.UUID.setTable(VOLUME_CONNECTIONS);
        VOLUME_CONNECTIONS.NODE_NAME_SRC.setTable(VOLUME_CONNECTIONS);
        VOLUME_CONNECTIONS.NODE_NAME_DST.setTable(VOLUME_CONNECTIONS);
        VOLUME_CONNECTIONS.RESOURCE_NAME.setTable(VOLUME_CONNECTIONS);
        VOLUME_CONNECTIONS.VLM_NR.setTable(VOLUME_CONNECTIONS);
        VOLUME_DEFINITIONS.UUID.setTable(VOLUME_DEFINITIONS);
        VOLUME_DEFINITIONS.RESOURCE_NAME.setTable(VOLUME_DEFINITIONS);
        VOLUME_DEFINITIONS.VLM_NR.setTable(VOLUME_DEFINITIONS);
        VOLUME_DEFINITIONS.VLM_SIZE.setTable(VOLUME_DEFINITIONS);
        VOLUME_DEFINITIONS.VLM_FLAGS.setTable(VOLUME_DEFINITIONS);
        VOLUME_GROUPS.UUID.setTable(VOLUME_GROUPS);
        VOLUME_GROUPS.RESOURCE_GROUP_NAME.setTable(VOLUME_GROUPS);
        VOLUME_GROUPS.VLM_NR.setTable(VOLUME_GROUPS);
    }
}
