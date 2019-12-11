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
        String getName();
    }

    private GeneratedDatabaseTables()
    {
    }

    // Schema name
    public static final String DATABASE_SCHEMA_NAME = "LINSTOR";

    public static class KeyValueStore implements Table
    {
        private KeyValueStore() { }

        // Primary Key
        public static final Column KVS_NAME = new Column("KVS_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column KVS_DSP_NAME = new Column("KVS_DSP_NAME", Types.VARCHAR, false, false);

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
    }

    public static class LayerDrbdResources implements Table
    {
        private LayerDrbdResources() { }

        // Primary Key
        public static final Column LAYER_RESOURCE_ID = new Column("LAYER_RESOURCE_ID", Types.INTEGER, true, false);

        public static final Column PEER_SLOTS = new Column("PEER_SLOTS", Types.INTEGER, false, false);
        public static final Column AL_STRIPES = new Column("AL_STRIPES", Types.INTEGER, false, false);
        public static final Column AL_STRIPE_SIZE = new Column("AL_STRIPE_SIZE", Types.BIGINT, false, false);
        public static final Column FLAGS = new Column("FLAGS", Types.BIGINT, false, false);
        public static final Column NODE_ID = new Column("NODE_ID", Types.INTEGER, false, false);

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
    }

    public static class LayerDrbdResourceDefinitions implements Table
    {
        private LayerDrbdResourceDefinitions() { }

        // Primary Keys
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column RESOURCE_NAME_SUFFIX = new Column("RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final Column PEER_SLOTS = new Column("PEER_SLOTS", Types.INTEGER, false, false);
        public static final Column AL_STRIPES = new Column("AL_STRIPES", Types.INTEGER, false, false);
        public static final Column AL_STRIPE_SIZE = new Column("AL_STRIPE_SIZE", Types.BIGINT, false, false);
        public static final Column TCP_PORT = new Column("TCP_PORT", Types.INTEGER, false, true);
        public static final Column TRANSPORT_TYPE = new Column("TRANSPORT_TYPE", Types.VARCHAR, false, false);
        public static final Column SECRET = new Column("SECRET", Types.VARCHAR, false, true);

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
    }

    public static class LayerDrbdVolumes implements Table
    {
        private LayerDrbdVolumes() { }

        // Primary Keys
        public static final Column LAYER_RESOURCE_ID = new Column("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, false, true);
        public static final Column POOL_NAME = new Column("POOL_NAME", Types.VARCHAR, false, true);

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
    }

    public static class LayerDrbdVolumeDefinitions implements Table
    {
        private LayerDrbdVolumeDefinitions() { }

        // Primary Keys
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column RESOURCE_NAME_SUFFIX = new Column("RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column VLM_MINOR_NR = new Column("VLM_MINOR_NR", Types.INTEGER, false, true);

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
    }

    public static class LayerLuksVolumes implements Table
    {
        private LayerLuksVolumes() { }

        // Primary Keys
        public static final Column LAYER_RESOURCE_ID = new Column("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column ENCRYPTED_PASSWORD = new Column("ENCRYPTED_PASSWORD", Types.VARCHAR, false, false);

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
    }

    public static class LayerResourceIds implements Table
    {
        private LayerResourceIds() { }

        // Primary Key
        public static final Column LAYER_RESOURCE_ID = new Column("LAYER_RESOURCE_ID", Types.INTEGER, true, false);

        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, false, false);
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, false, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, false, false);
        public static final Column LAYER_RESOURCE_KIND = new Column("LAYER_RESOURCE_KIND", Types.VARCHAR, false, false);
        public static final Column LAYER_RESOURCE_PARENT_ID = new Column("LAYER_RESOURCE_PARENT_ID", Types.INTEGER, false, true);
        public static final Column LAYER_RESOURCE_SUFFIX = new Column("LAYER_RESOURCE_SUFFIX", Types.VARCHAR, false, false);
        public static final Column LAYER_RESOURCE_SUSPENDED = new Column("LAYER_RESOURCE_SUSPENDED", Types.BOOLEAN, false, false);

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
    }

    public static class LayerStorageVolumes implements Table
    {
        private LayerStorageVolumes() { }

        // Primary Keys
        public static final Column LAYER_RESOURCE_ID = new Column("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column PROVIDER_KIND = new Column("PROVIDER_KIND", Types.VARCHAR, false, false);
        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, false, false);
        public static final Column STOR_POOL_NAME = new Column("STOR_POOL_NAME", Types.VARCHAR, false, false);

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
    }

    public static class LayerSwordfishVolumeDefinitions implements Table
    {
        private LayerSwordfishVolumeDefinitions() { }

        // Primary Keys
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final Column RESOURCE_NAME_SUFFIX = new Column("RESOURCE_NAME_SUFFIX", Types.VARCHAR, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column VLM_ODATA = new Column("VLM_ODATA", Types.VARCHAR, false, true);

        public static final Column[] ALL = new Column[]
        {
            RESOURCE_NAME,
            SNAPSHOT_NAME,
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
        public String getName()
        {
            return "LAYER_SWORDFISH_VOLUME_DEFINITIONS";
        }
    }

    public static class LayerWritecacheVolumes implements Table
    {
        private LayerWritecacheVolumes() { }

        // Primary Keys
        public static final Column LAYER_RESOURCE_ID = new Column("LAYER_RESOURCE_ID", Types.INTEGER, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, false, false);
        public static final Column POOL_NAME = new Column("POOL_NAME", Types.VARCHAR, false, false);

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
    }

    public static class Nodes implements Table
    {
        private Nodes() { }

        // Primary Key
        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column NODE_DSP_NAME = new Column("NODE_DSP_NAME", Types.VARCHAR, false, false);
        public static final Column NODE_FLAGS = new Column("NODE_FLAGS", Types.BIGINT, false, false);
        public static final Column NODE_TYPE = new Column("NODE_TYPE", Types.INTEGER, false, false);

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
    }

    public static class NodeConnections implements Table
    {
        private NodeConnections() { }

        // Primary Keys
        public static final Column NODE_NAME_SRC = new Column("NODE_NAME_SRC", Types.VARCHAR, true, false);
        public static final Column NODE_NAME_DST = new Column("NODE_NAME_DST", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);

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
    }

    public static class NodeNetInterfaces implements Table
    {
        private NodeNetInterfaces() { }

        // Primary Keys
        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, true, false);
        public static final Column NODE_NET_NAME = new Column("NODE_NET_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column NODE_NET_DSP_NAME = new Column("NODE_NET_DSP_NAME", Types.VARCHAR, false, false);
        public static final Column INET_ADDRESS = new Column("INET_ADDRESS", Types.VARCHAR, false, false);
        public static final Column STLT_CONN_PORT = new Column("STLT_CONN_PORT", Types.SMALLINT, false, true);
        public static final Column STLT_CONN_ENCR_TYPE = new Column("STLT_CONN_ENCR_TYPE", Types.VARCHAR, false, true);

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
    }

    public static class NodeStorPool implements Table
    {
        private NodeStorPool() { }

        // Primary Keys
        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, true, false);
        public static final Column POOL_NAME = new Column("POOL_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column DRIVER_NAME = new Column("DRIVER_NAME", Types.VARCHAR, false, false);
        public static final Column FREE_SPACE_MGR_NAME = new Column("FREE_SPACE_MGR_NAME", Types.VARCHAR, false, false);
        public static final Column FREE_SPACE_MGR_DSP_NAME = new Column("FREE_SPACE_MGR_DSP_NAME", Types.VARCHAR, false, false);

        public static final Column[] ALL = new Column[]
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
        public String getName()
        {
            return "NODE_STOR_POOL";
        }
    }

    public static class PropsContainers implements Table
    {
        private PropsContainers() { }

        // Primary Keys
        public static final Column PROPS_INSTANCE = new Column("PROPS_INSTANCE", Types.VARCHAR, true, false);
        public static final Column PROP_KEY = new Column("PROP_KEY", Types.VARCHAR, true, false);

        public static final Column PROP_VALUE = new Column("PROP_VALUE", Types.VARCHAR, false, false);

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
    }

    public static class Resources implements Table
    {
        private Resources() { }

        // Primary Keys
        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, true, false);
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column RESOURCE_FLAGS = new Column("RESOURCE_FLAGS", Types.BIGINT, false, false);

        public static final Column[] ALL = new Column[]
        {
            UUID,
            NODE_NAME,
            RESOURCE_NAME,
            SNAPSHOT_NAME,
            RESOURCE_FLAGS
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
    }

    public static class ResourceConnections implements Table
    {
        private ResourceConnections() { }

        // Primary Keys
        public static final Column NODE_NAME_SRC = new Column("NODE_NAME_SRC", Types.VARCHAR, true, false);
        public static final Column NODE_NAME_DST = new Column("NODE_NAME_DST", Types.VARCHAR, true, false);
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column FLAGS = new Column("FLAGS", Types.BIGINT, false, false);
        public static final Column TCP_PORT = new Column("TCP_PORT", Types.INTEGER, false, true);

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
    }

    public static class ResourceDefinitions implements Table
    {
        private ResourceDefinitions() { }

        // Primary Keys
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column RESOURCE_DSP_NAME = new Column("RESOURCE_DSP_NAME", Types.VARCHAR, false, true);
        public static final Column SNAPSHOT_DSP_NAME = new Column("SNAPSHOT_DSP_NAME", Types.VARCHAR, false, false);
        public static final Column RESOURCE_FLAGS = new Column("RESOURCE_FLAGS", Types.BIGINT, false, false);
        public static final Column LAYER_STACK = new Column("LAYER_STACK", Types.VARCHAR, false, false);
        public static final Column RESOURCE_EXTERNAL_NAME = new Column("RESOURCE_EXTERNAL_NAME", Types.BLOB, false, true);
        public static final Column RESOURCE_GROUP_NAME = new Column("RESOURCE_GROUP_NAME", Types.VARCHAR, false, false);
        public static final Column PARENT_UUID = new Column("PARENT_UUID", Types.CHAR, false, true);

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
    }

    public static class ResourceGroups implements Table
    {
        private ResourceGroups() { }

        // Primary Key
        public static final Column RESOURCE_GROUP_NAME = new Column("RESOURCE_GROUP_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column RESOURCE_GROUP_DSP_NAME = new Column("RESOURCE_GROUP_DSP_NAME", Types.VARCHAR, false, false);
        public static final Column DESCRIPTION = new Column("DESCRIPTION", Types.VARCHAR, false, true);
        public static final Column LAYER_STACK = new Column("LAYER_STACK", Types.VARCHAR, false, true);
        public static final Column REPLICA_COUNT = new Column("REPLICA_COUNT", Types.INTEGER, false, true);
        public static final Column POOL_NAME = new Column("POOL_NAME", Types.VARCHAR, false, true);
        public static final Column DO_NOT_PLACE_WITH_RSC_REGEX = new Column("DO_NOT_PLACE_WITH_RSC_REGEX", Types.VARCHAR, false, true);
        public static final Column DO_NOT_PLACE_WITH_RSC_LIST = new Column("DO_NOT_PLACE_WITH_RSC_LIST", Types.VARCHAR, false, true);
        public static final Column REPLICAS_ON_SAME = new Column("REPLICAS_ON_SAME", Types.BLOB, false, true);
        public static final Column REPLICAS_ON_DIFFERENT = new Column("REPLICAS_ON_DIFFERENT", Types.BLOB, false, true);
        public static final Column ALLOWED_PROVIDER_LIST = new Column("ALLOWED_PROVIDER_LIST", Types.VARCHAR, false, true);
        public static final Column DISKLESS_ON_REMAINING = new Column("DISKLESS_ON_REMAINING", Types.BOOLEAN, false, true);

        public static final Column[] ALL = new Column[]
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
        public String getName()
        {
            return "RESOURCE_GROUPS";
        }
    }

    public static class SecAccessTypes implements Table
    {
        private SecAccessTypes() { }

        // Primary Key
        public static final Column ACCESS_TYPE_NAME = new Column("ACCESS_TYPE_NAME", Types.VARCHAR, true, false);

        public static final Column ACCESS_TYPE_VALUE = new Column("ACCESS_TYPE_VALUE", Types.SMALLINT, false, false);

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
    }

    public static class SecAclMap implements Table
    {
        private SecAclMap() { }

        // Primary Keys
        public static final Column OBJECT_PATH = new Column("OBJECT_PATH", Types.VARCHAR, true, false);
        public static final Column ROLE_NAME = new Column("ROLE_NAME", Types.VARCHAR, true, false);

        public static final Column ACCESS_TYPE = new Column("ACCESS_TYPE", Types.SMALLINT, false, false);

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
    }

    public static class SecConfiguration implements Table
    {
        private SecConfiguration() { }

        // Primary Key
        public static final Column ENTRY_KEY = new Column("ENTRY_KEY", Types.VARCHAR, true, false);

        public static final Column ENTRY_DSP_KEY = new Column("ENTRY_DSP_KEY", Types.VARCHAR, false, false);
        public static final Column ENTRY_VALUE = new Column("ENTRY_VALUE", Types.VARCHAR, false, false);

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
    }

    public static class SecDfltRoles implements Table
    {
        private SecDfltRoles() { }

        // Primary Key
        public static final Column IDENTITY_NAME = new Column("IDENTITY_NAME", Types.VARCHAR, true, false);

        public static final Column ROLE_NAME = new Column("ROLE_NAME", Types.VARCHAR, false, false);

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
    }

    public static class SecIdentities implements Table
    {
        private SecIdentities() { }

        // Primary Key
        public static final Column IDENTITY_NAME = new Column("IDENTITY_NAME", Types.VARCHAR, true, false);

        public static final Column IDENTITY_DSP_NAME = new Column("IDENTITY_DSP_NAME", Types.VARCHAR, false, false);
        public static final Column PASS_SALT = new Column("PASS_SALT", Types.CHAR, false, true);
        public static final Column PASS_HASH = new Column("PASS_HASH", Types.CHAR, false, true);
        public static final Column ID_ENABLED = new Column("ID_ENABLED", Types.BOOLEAN, false, false);
        public static final Column ID_LOCKED = new Column("ID_LOCKED", Types.BOOLEAN, false, false);

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
    }

    public static class SecIdRoleMap implements Table
    {
        private SecIdRoleMap() { }

        // Primary Keys
        public static final Column IDENTITY_NAME = new Column("IDENTITY_NAME", Types.VARCHAR, true, false);
        public static final Column ROLE_NAME = new Column("ROLE_NAME", Types.VARCHAR, true, false);

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
    }

    public static class SecObjectProtection implements Table
    {
        private SecObjectProtection() { }

        // Primary Key
        public static final Column OBJECT_PATH = new Column("OBJECT_PATH", Types.VARCHAR, true, false);

        public static final Column CREATOR_IDENTITY_NAME = new Column("CREATOR_IDENTITY_NAME", Types.VARCHAR, false, false);
        public static final Column OWNER_ROLE_NAME = new Column("OWNER_ROLE_NAME", Types.VARCHAR, false, false);
        public static final Column SECURITY_TYPE_NAME = new Column("SECURITY_TYPE_NAME", Types.VARCHAR, false, false);

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
    }

    public static class SecRoles implements Table
    {
        private SecRoles() { }

        // Primary Key
        public static final Column ROLE_NAME = new Column("ROLE_NAME", Types.VARCHAR, true, false);

        public static final Column ROLE_DSP_NAME = new Column("ROLE_DSP_NAME", Types.VARCHAR, false, false);
        public static final Column DOMAIN_NAME = new Column("DOMAIN_NAME", Types.VARCHAR, false, false);
        public static final Column ROLE_ENABLED = new Column("ROLE_ENABLED", Types.BOOLEAN, false, false);
        public static final Column ROLE_PRIVILEGES = new Column("ROLE_PRIVILEGES", Types.BIGINT, false, false);

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
    }

    public static class SecTypes implements Table
    {
        private SecTypes() { }

        // Primary Key
        public static final Column TYPE_NAME = new Column("TYPE_NAME", Types.VARCHAR, true, false);

        public static final Column TYPE_DSP_NAME = new Column("TYPE_DSP_NAME", Types.VARCHAR, false, false);
        public static final Column TYPE_ENABLED = new Column("TYPE_ENABLED", Types.BOOLEAN, false, false);

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
    }

    public static class SecTypeRules implements Table
    {
        private SecTypeRules() { }

        // Primary Keys
        public static final Column DOMAIN_NAME = new Column("DOMAIN_NAME", Types.VARCHAR, true, false);
        public static final Column TYPE_NAME = new Column("TYPE_NAME", Types.VARCHAR, true, false);

        public static final Column ACCESS_TYPE = new Column("ACCESS_TYPE", Types.SMALLINT, false, false);

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
    }

    public static class StorPoolDefinitions implements Table
    {
        private StorPoolDefinitions() { }

        // Primary Key
        public static final Column POOL_NAME = new Column("POOL_NAME", Types.VARCHAR, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column POOL_DSP_NAME = new Column("POOL_DSP_NAME", Types.VARCHAR, false, false);

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
    }

    public static class Volumes implements Table
    {
        private Volumes() { }

        // Primary Keys
        public static final Column NODE_NAME = new Column("NODE_NAME", Types.VARCHAR, true, false);
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column VLM_FLAGS = new Column("VLM_FLAGS", Types.BIGINT, false, false);

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
    }

    public static class VolumeConnections implements Table
    {
        private VolumeConnections() { }

        // Primary Keys
        public static final Column NODE_NAME_SRC = new Column("NODE_NAME_SRC", Types.VARCHAR, true, false);
        public static final Column NODE_NAME_DST = new Column("NODE_NAME_DST", Types.VARCHAR, true, false);
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);

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
    }

    public static class VolumeDefinitions implements Table
    {
        private VolumeDefinitions() { }

        // Primary Keys
        public static final Column RESOURCE_NAME = new Column("RESOURCE_NAME", Types.VARCHAR, true, false);
        public static final Column SNAPSHOT_NAME = new Column("SNAPSHOT_NAME", Types.VARCHAR, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);
        public static final Column VLM_SIZE = new Column("VLM_SIZE", Types.BIGINT, false, false);
        public static final Column VLM_FLAGS = new Column("VLM_FLAGS", Types.BIGINT, false, false);

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
    }

    public static class VolumeGroups implements Table
    {
        private VolumeGroups() { }

        // Primary Keys
        public static final Column RESOURCE_GROUP_NAME = new Column("RESOURCE_GROUP_NAME", Types.VARCHAR, true, false);
        public static final Column VLM_NR = new Column("VLM_NR", Types.INTEGER, true, false);

        public static final Column UUID = new Column("UUID", Types.CHAR, false, false);

        public static final Column[] ALL = new Column[]
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
        public String getName()
        {
            return "VOLUME_GROUPS";
        }
    }

    public static final KeyValueStore KEY_VALUE_STORE = new KeyValueStore();
    public static final LayerDrbdResources LAYER_DRBD_RESOURCES = new LayerDrbdResources();
    public static final LayerDrbdResourceDefinitions LAYER_DRBD_RESOURCE_DEFINITIONS = new LayerDrbdResourceDefinitions();
    public static final LayerDrbdVolumes LAYER_DRBD_VOLUMES = new LayerDrbdVolumes();
    public static final LayerDrbdVolumeDefinitions LAYER_DRBD_VOLUME_DEFINITIONS = new LayerDrbdVolumeDefinitions();
    public static final LayerLuksVolumes LAYER_LUKS_VOLUMES = new LayerLuksVolumes();
    public static final LayerResourceIds LAYER_RESOURCE_IDS = new LayerResourceIds();
    public static final LayerStorageVolumes LAYER_STORAGE_VOLUMES = new LayerStorageVolumes();
    public static final LayerSwordfishVolumeDefinitions LAYER_SWORDFISH_VOLUME_DEFINITIONS = new LayerSwordfishVolumeDefinitions();
    public static final LayerWritecacheVolumes LAYER_WRITECACHE_VOLUMES = new LayerWritecacheVolumes();
    public static final Nodes NODES = new Nodes();
    public static final NodeConnections NODE_CONNECTIONS = new NodeConnections();
    public static final NodeNetInterfaces NODE_NET_INTERFACES = new NodeNetInterfaces();
    public static final NodeStorPool NODE_STOR_POOL = new NodeStorPool();
    public static final PropsContainers PROPS_CONTAINERS = new PropsContainers();
    public static final Resources RESOURCES = new Resources();
    public static final ResourceConnections RESOURCE_CONNECTIONS = new ResourceConnections();
    public static final ResourceDefinitions RESOURCE_DEFINITIONS = new ResourceDefinitions();
    public static final ResourceGroups RESOURCE_GROUPS = new ResourceGroups();
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
    public static final StorPoolDefinitions STOR_POOL_DEFINITIONS = new StorPoolDefinitions();
    public static final Volumes VOLUMES = new Volumes();
    public static final VolumeConnections VOLUME_CONNECTIONS = new VolumeConnections();
    public static final VolumeDefinitions VOLUME_DEFINITIONS = new VolumeDefinitions();
    public static final VolumeGroups VOLUME_GROUPS = new VolumeGroups();

    static
    {
        KeyValueStore.UUID.table = KEY_VALUE_STORE;
        KeyValueStore.KVS_NAME.table = KEY_VALUE_STORE;
        KeyValueStore.KVS_DSP_NAME.table = KEY_VALUE_STORE;
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
        LayerSwordfishVolumeDefinitions.RESOURCE_NAME.table = LAYER_SWORDFISH_VOLUME_DEFINITIONS;
        LayerSwordfishVolumeDefinitions.SNAPSHOT_NAME.table = LAYER_SWORDFISH_VOLUME_DEFINITIONS;
        LayerSwordfishVolumeDefinitions.RESOURCE_NAME_SUFFIX.table = LAYER_SWORDFISH_VOLUME_DEFINITIONS;
        LayerSwordfishVolumeDefinitions.VLM_NR.table = LAYER_SWORDFISH_VOLUME_DEFINITIONS;
        LayerSwordfishVolumeDefinitions.VLM_ODATA.table = LAYER_SWORDFISH_VOLUME_DEFINITIONS;
        LayerWritecacheVolumes.LAYER_RESOURCE_ID.table = LAYER_WRITECACHE_VOLUMES;
        LayerWritecacheVolumes.VLM_NR.table = LAYER_WRITECACHE_VOLUMES;
        LayerWritecacheVolumes.NODE_NAME.table = LAYER_WRITECACHE_VOLUMES;
        LayerWritecacheVolumes.POOL_NAME.table = LAYER_WRITECACHE_VOLUMES;
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
        PropsContainers.PROPS_INSTANCE.table = PROPS_CONTAINERS;
        PropsContainers.PROP_KEY.table = PROPS_CONTAINERS;
        PropsContainers.PROP_VALUE.table = PROPS_CONTAINERS;
        Resources.UUID.table = RESOURCES;
        Resources.NODE_NAME.table = RESOURCES;
        Resources.RESOURCE_NAME.table = RESOURCES;
        Resources.SNAPSHOT_NAME.table = RESOURCES;
        Resources.RESOURCE_FLAGS.table = RESOURCES;
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
        ResourceGroups.POOL_NAME.table = RESOURCE_GROUPS;
        ResourceGroups.DO_NOT_PLACE_WITH_RSC_REGEX.table = RESOURCE_GROUPS;
        ResourceGroups.DO_NOT_PLACE_WITH_RSC_LIST.table = RESOURCE_GROUPS;
        ResourceGroups.REPLICAS_ON_SAME.table = RESOURCE_GROUPS;
        ResourceGroups.REPLICAS_ON_DIFFERENT.table = RESOURCE_GROUPS;
        ResourceGroups.ALLOWED_PROVIDER_LIST.table = RESOURCE_GROUPS;
        ResourceGroups.DISKLESS_ON_REMAINING.table = RESOURCE_GROUPS;
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
        StorPoolDefinitions.UUID.table = STOR_POOL_DEFINITIONS;
        StorPoolDefinitions.POOL_NAME.table = STOR_POOL_DEFINITIONS;
        StorPoolDefinitions.POOL_DSP_NAME.table = STOR_POOL_DEFINITIONS;
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
    }

    public static class Column
    {
        private final String name;
        private final int sqlType;
        private final boolean isPk;
        private final boolean isNullable;
        private Table table;

        public Column(
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

        public String getName()
        {
            return name;
        }

        public int getSqlType()
        {
            return sqlType;
        }

        public boolean isPk()
        {
            return isPk;
        }

        public boolean isNullable()
        {
            return isNullable;
        }

        public Table getTable()
        {
            return table;
        }
    }
}
