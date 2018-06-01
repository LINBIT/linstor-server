package com.linbit.linstor.dbdrivers.derby;

public class DbConstants
{
    // Schema name
    public static final String DATABASE_SCHEMA_NAME = "LINSTOR";

    // View names
    public static final String VIEW_SEC_IDENTITIES_LOAD = "SEC_IDENTITIES_LOAD";
    public static final String VIEW_SEC_ROLES_LOAD      = "SEC_ROLES_LOAD";
    public static final String VIEW_SEC_TYPES_LOAD      = "SEC_TYPES_LOAD";
    public static final String VIEW_SEC_TYPE_RULES_LOAD = "SEC_TYPE_RULES_LOAD";

    // Table names
    public static final String TBL_SEC_CONFIGURATION     = "SEC_CONFIGURATION";
    public static final String TBL_SEC_IDENTITIES        = "SEC_IDENTITIES";
    public static final String TBL_SEC_TYPES             = "SEC_TYPES";
    public static final String TBL_SEC_ROLES             = "SEC_ROLES";
    public static final String TBL_SEC_ID_ROLE_MAP       = "SEC_ID_ROLE_MAP";
    public static final String TBL_SEC_ACCESS_TYPES      = "SEC_ACCESS_TYPES";
    public static final String TBL_SEC_TYPE_RULES        = "SEC_TYPE_RULES";
    public static final String TBL_SEC_DFLT_ROLES        = "SEC_DFLT_ROLES";
    public static final String TBL_SEC_OBJECT_PROTECTION = "SEC_OBJECT_PROTECTION";
    public static final String TBL_SEC_ACL_MAP           = "SEC_ACL_MAP";
    public static final String TBL_NODES                 = "NODES";
    public static final String TBL_NODE_NET_INTERFACES   = "NODE_NET_INTERFACES";
    public static final String TBL_SATELLITE_CONNECTIONS = "SATELLITE_CONNECTIONS";
    public static final String TBL_RESOURCE_DEFINITIONS  = "RESOURCE_DEFINITIONS";
    public static final String TBL_RESOURCES             = "RESOURCES";
    public static final String TBL_STOR_POOL_DEFINITIONS = "STOR_POOL_DEFINITIONS";
    public static final String TBL_NODE_STOR_POOL        = "NODE_STOR_POOL";
    public static final String TBL_VOLUME_DEFINITIONS    = "VOLUME_DEFINITIONS";
    public static final String TBL_VOLUMES               = "VOLUMES";
    public static final String TBL_NODE_CONNECTIONS      = "NODE_CONNECTIONS";
    public static final String TBL_RESOURCE_CONNECTIONS  = "RESOURCE_CONNECTIONS";
    public static final String TBL_VOLUME_CONNECTIONS    = "VOLUME_CONNECTIONS";
    public static final String TBL_PROPS_CONTAINERS      = "PROPS_CONTAINERS";
    public static final String TBL_SNAPSHOT_DEFINITIONS  = "SNAPSHOT_DEFINITIONS";
    public static final String TBL_SNAPSHOTS             = "SNAPSHOTS";
    public static final String TBL_SNAPSHOT_VOLUME_DEFINITIONS = "SNAPSHOT_VOLUME_DEFINITIONS";

    // SEC_CONFIGURATION column names
    public static final String ENTRY_KEY     = "ENTRY_KEY";
    public static final String ENTRY_DSP_KEY = "ENTRY_DSP_KEY";
    public static final String ENTRY_VALUE   = "ENTRY_VALUE";

    // SEC_IDENTITIES column names
    public static final String IDENTITY_NAME     = "IDENTITY_NAME";
    public static final String IDENTITY_DSP_NAME = "IDENTITY_DSP_NAME";
    public static final String PASS_SALT         = "PASS_SALT";
    public static final String PASS_HASH         = "PASS_HASH";
    public static final String ID_ENABLED        = "ID_ENABLED";
    public static final String ID_LOCKED         = "ID_LOCKED";

    // SEC_TYPES column names
    public static final String TYPE_NAME     = "TYPE_NAME";
    public static final String TYPE_DSP_NAME = "TYPE_DSP_NAME";
    public static final String TYPE_ENABLED  = "TYPE_ENABLED";

    // SEC_ROLES column names
    public static final String ROLE_NAME       = "ROLE_NAME";
    public static final String ROLE_DSP_NAME   = "ROLE_DSP_NAME";
    public static final String DOMAIN_NAME     = "DOMAIN_NAME";
    public static final String ROLE_ENABLED    = "ROLE_ENABLED";
    public static final String ROLE_PRIVILEGES = "ROLE_PRIVILEGES";

    // SEC_ACCESS_TYPES column names
    public static final String ACCESS_TYPE_NAME  = "ACCESS_TYPE_NAME";
    public static final String ACCESS_TYPE_VALUE = "ACCESS_TYPE_VALUE";

    // SEC_TYPE_RULES column names
    public static final String ACCESS_TYPE = "ACCESS_TYPE";

    // SEC_OBJECT_PROTECTION column names
    public static final String OBJECT_PATH           = "OBJECT_PATH";
    public static final String CREATOR_IDENTITY_NAME = "CREATOR_IDENTITY_NAME";
    public static final String OWNER_ROLE_NAME       = "OWNER_ROLE_NAME";
    public static final String SECURITY_TYPE_NAME    = "SECURITY_TYPE_NAME";

    // NODES column names
    public static final String UUID          = "UUID";
    public static final String NODE_NAME     = "NODE_NAME";
    public static final String NODE_DSP_NAME = "NODE_DSP_NAME";
    public static final String NODE_FLAGS    = "NODE_FLAGS";
    public static final String NODE_TYPE     = "NODE_TYPE";

    // NODE_NET_INTERFACES column names
    public static final String NODE_NET_NAME       = "NODE_NET_NAME";
    public static final String NODE_NET_DSP_NAME   = "NODE_NET_DSP_NAME";
    public static final String INET_ADDRESS        = "INET_ADDRESS";
    public static final String STLT_CONN_PORT      = "STLT_CONN_PORT";
    public static final String STLT_CONN_ENCR_TYPE = "STLT_CONN_ENCR_TYPE";

    // SATELLITE_CONNECTIONS column names
    public static final String TCP_PORT      = "TCP_PORT";
    public static final String INET_TYPE     = "INET_TYPE";

    // RESOURCE_DEFINITIONS column names
    public static final String RESOURCE_NAME     = "RESOURCE_NAME";
    public static final String RESOURCE_DSP_NAME = "RESOURCE_DSP_NAME";
    public static final String RESOURCE_FLAGS    = "RESOURCE_FLAGS";
    public static final String SECRET            = "SECRET";
    public static final String TRANSPORT_TYPE    = "TRANSPORT_TYPE";

    // RESOURCES column names
    public static final String NODE_ID        = "NODE_ID";

    // STOR_POOL_DEFINITIONS column names
    public static final String POOL_NAME     = "POOL_NAME";
    public static final String POOL_DSP_NAME = "POOL_DSP_NAME";

    // NODE_STOR_POOL column names
    public static final String DRIVER_NAME = "DRIVER_NAME";

    // VOLUME_DEFINITIONS column names
    public static final String VLM_NR        = "VLM_NR";
    public static final String VLM_SIZE      = "VLM_SIZE";
    public static final String VLM_MINOR_NR  = "VLM_MINOR_NR";
    public static final String VLM_FLAGS     = "VLM_FLAGS";

    // VOLUMES column names
    public static final String STOR_POOL_NAME    = "STOR_POOL_NAME";

    // NODE_CONNECTIONS column names
    public static final String NODE_NAME_SRC = "NODE_NAME_SRC";
    public static final String NODE_NAME_DST = "NODE_NAME_DST";

    // PROPS_CONTAINERS column names
    public static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    public static final String PROP_KEY       = "PROP_KEY";
    public static final String PROP_VALUE     = "PROP_VALUE";

    // SNAPSHOT_DEFINITIONS column names
    public static final String SNAPSHOT_NAME = "SNAPSHOT_NAME";
    public static final String SNAPSHOT_DSP_NAME = "SNAPSHOT_DSP_NAME";
    public static final String SNAPSHOT_FLAGS = "SNAPSHOT_FLAGS";

    private DbConstants()
    {
    }
}
