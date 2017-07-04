package com.linbit.drbdmanage.security;

public interface DerbyConstants
{
    // View names
    public static final String VIEW_SEC_IDENTITIES_LOAD = "SEC_IDENTITIES_LOAD";
    public static final String VIEW_SEC_ROLES_LOAD      = "SEC_ROLES_LOAD";
    public static final String VIEW_SEC_TYPES_LOAD      = "SEC_TYPES_LOAD";
    public static final String VIEW_SEC_TYPE_RULES_LOAD = "SEC_TYPE_RULES_LOAD";

    // Table names
    public static final String TBL_SEC_CONFIGURATION      = "SEC_CONFIGURATION";
    public static final String TBL_SEC_IDENTITIES         = "SEC_IDENTITIES";
    public static final String TBL_SEC_TYPES              = "SEC_TYPES";
    public static final String TBL_SEC_ROLES              = "SEC_ROLES";
    public static final String TBL_SEC_ID_ROLE_MAP        = "SEC_ID_ROLE_MAP";
    public static final String TBL_SEC_ACCESS_TYPES       = "SEC_ACCESS_TYPES";
    public static final String TBL_SEC_TYPE_RULES         = "SEC_TYPE_RULES";
    public static final String TBL_SEC_DFLT_ROLES         = "SEC_DFLT_ROLES";
    public static final String TBL_SEC_OBJECT_PROTECTION  = "SEC_OBJECT_PROTECTION";
    public static final String TBL_SEC_ACL_MAP            = "SEC_ACL_MAP";
    public static final String TBL_CTRL_CONFIGURATION     = "CTRL_CONFIGURATION";
    public static final String TBL_NODES                  = "NODES";
    public static final String TBL_NODE_NET_INTERFACES    = "NODE_NET_INTERFACES";
    public static final String TBL_RESOURCE_DEFINITIONS   = "RESOURCE_DEFINITIONS";
    public static final String TBL_NODE_RESOURCE          = "NODE_RESOURCE";
    public static final String TBL_VOLUME_DEFINITIONS     = "VOLUME_DEFINITIONS";
    public static final String TBL_VOLUMES                = "VOLUMES";
    public static final String TBL_STOR_POOL_DEFINITIONS  = "STOR_POOL_DEFINITIONS";
    public static final String TBL_NODE_STOR_POOL         = "NODE_STOR_POOL";
    public static final String TBL_CONNECTION_DEFINITIONS = "CONNECTION_DEFINITIONS";
    public static final String TBL_PROPS_CONTAINERS       = "PROPS_CONTAINERS";

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
    public static final String INET_TRANSPORT_TYPE = "INET_TRANSPORT_TYPE";

    // RESOURCE_DEFINITIONS column names
    public static final String RESOURCE_NAME     = "RESOURCE_NAME";
    public static final String RESOURCE_DSP_NAME = "RESOURCE_DSP_NAME";

    // NODE_RESOURCE column names
    public static final String NODE_ID       = "NODE_ID";
    public static final String RES_FLAGS     = "RES_FLAGS";

    // VOLUME_DEFINITIONS column names
    public static final String VLM_ID        = "VLM_ID";
    public static final String VLM_SIZE      = "VLM_SIZE";
    public static final String VLM_MINOR_NR  = "VLM_MINOR_NR";

    // VOLUMES column names
    public static final String BLOCK_DEVICE_PATH = "BLOCK_DEVICE_PATH";
    public static final String VLM_FLAGS         = "VLM_FLAGS";

    // STOR_POOL_DEFINITIONS column names
    public static final String POOL_NAME     = "POOL_NAME";
    public static final String POOL_DSP_NAME = "POOL_DSP_NAME";

    // NODE_STOR_POOL column names
    public static final String DRIVER_NAME = "DRIVER_NAME";

    // CONNECTION_DEFINITIONS column names
    public static final String NODE_NAME_SRC = "NODE_NAME_SRC";
    public static final String NODE_NAME_DST = "NODE_NAME_DST";

    // PROPS_CONTAINERS column names
    public static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    public static final String PROP_KEY       = "PROP_KEY";
    public static final String PROP_VALUE     = "PROP_VALUE";

    // table column counts
    public static final int TBL_COL_COUNT_SEC_CONFIGURATION      = 3;
    public static final int TBL_COL_COUNT_SEC_IDENTITIES         = 6;
    public static final int TBL_COL_COUNT_SEC_TYPES              = 3;
    public static final int TBL_COL_COUNT_SEC_ROLES              = 5;
    public static final int TBL_COL_COUNT_SEC_ID_ROLE_MAP        = 2;
    public static final int TBL_COL_COUNT_SEC_ACCESS_TYPES       = 2;
    public static final int TBL_COL_COUNT_SEC_TYPE_RULES         = 3;
    public static final int TBL_COL_COUNT_SEC_DFLT_ROLES         = 2;
    public static final int TBL_COL_COUNT_SEC_OBJECT_PROTECTION  = 4;
    public static final int TBL_COL_COUNT_SEC_ACL_MAP            = 3;
    public static final int TBL_COL_COUNT_CTRL_CONFIGURATION     = 3;
    public static final int TBL_COL_COUNT_NODES                  = 6;
    public static final int TBL_COL_COUNT_NODE_NET_INTERFACES    = 6;
    public static final int TBL_COL_COUNT_RESOURCE_DEFINITIONS   = 3;
    public static final int TBL_COL_COUNT_NODE_RESOURCE          = 5;
    public static final int TBL_COL_COUNT_VOLUME_DEFINITIONS     = 5;
    public static final int TBL_COL_COUNT_VOLUMES                = 6;
    public static final int TBL_COL_COUNT_STOR_POOL_DEFINITIONS  = 3;
    public static final int TBL_COL_COUNT_NODE_STOR_POOL         = 4;
    public static final int TBL_COL_COUNT_CONNECTION_DEFINITIONS = 4;
    public static final int TBL_COL_COUNT_PROPS_CONTAINERS       = 3;

    // create statements
    public static final String CREATE_TABLE_SEC_CONFIGURATION = 
        "CREATE TABLE SEC_CONFIGURATION \n" + 
        "( \n" + 
        "    ENTRY_KEY VARCHAR(24) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT SEC_CONF_CHKKEY CHECK (UPPER(ENTRY_KEY) = ENTRY_KEY AND LENGTH(ENTRY_KEY) >= 3), \n" + 
        "    ENTRY_DSP_KEY VARCHAR(24) NOT NULL, \n" + 
        "    ENTRY_VALUE VARCHAR(24) NOT NULL, \n" + 
        "        CONSTRAINT SEC_CONF_CHKDSPKEY CHECK (UPPER(ENTRY_DSP_KEY) = ENTRY_KEY) \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_IDENTITIES = 
        "CREATE TABLE SEC_IDENTITIES \n" + 
        "( \n" + 
        "    IDENTITY_NAME VARCHAR(24) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT SEC_ID_CHKNAME CHECK (UPPER(IDENTITY_NAME) = IDENTITY_NAME AND LENGTH(IDENTITY_NAME) >= 3), \n" + 
        "    IDENTITY_DSP_NAME VARCHAR(24) NOT NULL, \n" + 
        "    PASS_SALT CHAR(16) FOR BIT DATA, \n" + 
        "    PASS_HASH CHAR(64) FOR BIT DATA, \n" + 
        "    ID_ENABLED BOOLEAN NOT NULL DEFAULT TRUE, \n" + 
        "    ID_LOCKED BOOLEAN NOT NULL DEFAULT TRUE, \n" + 
        "        CONSTRAINT SEC_ID_CHKDSPNAME CHECK (UPPER(IDENTITY_DSP_NAME) = IDENTITY_NAME) \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_TYPES = 
        "CREATE TABLE SEC_TYPES \n" + 
        "( \n" + 
        "    TYPE_NAME VARCHAR(24) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT SEC_TYPES_CHKNAME CHECK (UPPER(TYPE_NAME) = TYPE_NAME AND LENGTH(TYPE_NAME) >= 3), \n" + 
        "    TYPE_DSP_NAME VARCHAR(24) NOT NULL, \n" + 
        "    TYPE_ENABLED BOOLEAN NOT NULL DEFAULT TRUE, \n" + 
        "    CONSTRAINT SEC_TYPES_CHKDSPNAME CHECK (UPPER(TYPE_DSP_NAME) = TYPE_NAME) \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_ROLES = 
        "CREATE TABLE SEC_ROLES \n" + 
        "( \n" + 
        "    ROLE_NAME VARCHAR(24) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT SEC_ROLES_CHKNAME CHECK (UPPER(ROLE_NAME) = ROLE_NAME AND LENGTH(ROLE_NAME) >= 3), \n" + 
        "    ROLE_DSP_NAME VARCHAR(24) NOT NULL, \n" + 
        "    DOMAIN_NAME VARCHAR(24) NOT NULL, \n" + 
        "    ROLE_ENABLED BOOLEAN NOT NULL DEFAULT TRUE, \n" + 
        "    ROLE_PRIVILEGES BIGINT NOT NULL DEFAULT 0, \n" + 
        "    FOREIGN KEY (DOMAIN_NAME) REFERENCES SEC_TYPES(TYPE_NAME), \n" + 
        "    CONSTRAINT SEC_ROLES_CHKDSPNAME CHECK (UPPER(ROLE_DSP_NAME) = ROLE_NAME) \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_ID_ROLE_MAP = 
        "CREATE TABLE SEC_ID_ROLE_MAP \n" + 
        "( \n" + 
        "    IDENTITY_NAME VARCHAR(24) NOT NULL, \n" + 
        "    ROLE_NAME VARCHAR(24) NOT NULL, \n" + 
        "    PRIMARY KEY (IDENTITY_NAME, ROLE_NAME), \n" + 
        "    FOREIGN KEY (IDENTITY_NAME) REFERENCES SEC_IDENTITIES(IDENTITY_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (ROLE_NAME) REFERENCES SEC_ROLES(ROLE_NAME) ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_ACCESS_TYPES = 
        "CREATE TABLE SEC_ACCESS_TYPES \n" + 
        "( \n" + 
        "    ACCESS_TYPE_NAME VARCHAR(24) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT SEC_ACCESS_TYPES_CHKNAME CHECK (UPPER(ACCESS_TYPE_NAME) = ACCESS_TYPE_NAME), \n" + 
        "    ACCESS_TYPE_VALUE SMALLINT NOT NULL UNIQUE \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_TYPE_RULES = 
        "CREATE TABLE SEC_TYPE_RULES \n" + 
        "( \n" + 
        "    DOMAIN_NAME VARCHAR(24) NOT NULL, \n" + 
        "    TYPE_NAME VARCHAR(24) NOT NULL, \n" + 
        "    ACCESS_TYPE SMALLINT NOT NULL, \n" + 
        "    PRIMARY KEY (DOMAIN_NAME, TYPE_NAME), \n" + 
        "    FOREIGN KEY (DOMAIN_NAME) REFERENCES SEC_TYPES(TYPE_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (TYPE_NAME) REFERENCES SEC_TYPES(TYPE_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (ACCESS_TYPE) REFERENCES SEC_ACCESS_TYPES(ACCESS_TYPE_VALUE) ON DELETE RESTRICT \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_DFLT_ROLES = 
        "CREATE TABLE SEC_DFLT_ROLES \n" + 
        "( \n" + 
        "    IDENTITY_NAME VARCHAR(24) NOT NULL PRIMARY KEY, \n" + 
        "    ROLE_NAME VARCHAR(24) NOT NULL, \n" + 
        "    FOREIGN KEY (IDENTITY_NAME, ROLE_NAME) REFERENCES SEC_ID_ROLE_MAP(IDENTITY_NAME, ROLE_NAME) \n" + 
        "        ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_OBJECT_PROTECTION = 
        "CREATE TABLE SEC_OBJECT_PROTECTION \n" + 
        "( \n" + 
        "    OBJECT_PATH VARCHAR(512) NOT NULL PRIMARY KEY, \n" + 
        "    CREATOR_IDENTITY_NAME VARCHAR(24) NOT NULL, \n" + 
        "    OWNER_ROLE_NAME VARCHAR(24) NOT NULL, \n" + 
        "    SECURITY_TYPE_NAME VARCHAR(24) NOT NULL, \n" + 
        "    FOREIGN KEY (CREATOR_IDENTITY_NAME) REFERENCES SEC_IDENTITIES(IDENTITY_NAME) ON DELETE RESTRICT, \n" + 
        "    FOREIGN KEY (OWNER_ROLE_NAME) REFERENCES SEC_ROLES(ROLE_NAME) ON DELETE RESTRICT, \n" + 
        "    FOREIGN KEY (SECURITY_TYPE_NAME) REFERENCES SEC_TYPES(TYPE_NAME) ON DELETE RESTRICT \n" + 
        ")";
    public static final String CREATE_TABLE_SEC_ACL_MAP = 
        "CREATE TABLE SEC_ACL_MAP \n" + 
        "( \n" + 
        "    OBJECT_PATH VARCHAR(512) NOT NULL, \n" + 
        "    ROLE_NAME VARCHAR(24) NOT NULL, \n" + 
        "    ACCESS_TYPE SMALLINT NOT NULL, \n" + 
        "    PRIMARY KEY (OBJECT_PATH, ROLE_NAME), \n" + 
        "    FOREIGN KEY (OBJECT_PATH) REFERENCES SEC_OBJECT_PROTECTION(OBJECT_PATH) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (ROLE_NAME) REFERENCES SEC_ROLES(ROLE_NAME) ON DELETE RESTRICT, \n" + 
        "    FOREIGN KEY (ACCESS_TYPE) REFERENCES SEC_ACCESS_TYPES(ACCESS_TYPE_VALUE) ON DELETE RESTRICT \n" + 
        ")";
    public static final String CREATE_TABLE_CTRL_CONFIGURATION = 
        "CREATE TABLE CTRL_CONFIGURATION \n" + 
        "( \n" + 
        "    ENTRY_KEY VARCHAR(512) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT CTRL_CONF_CHKKEY CHECK (UPPER(ENTRY_KEY) = ENTRY_KEY AND LENGTH(ENTRY_KEY) >= 1), \n" + 
        "    ENTRY_VALUE VARCHAR(512) NOT NULL, \n" + 
        "    ENTRY_DSP_KEY VARCHAR(512) NOT NULL, \n" + 
        "        CONSTRAINT CTRL_CONF_CHKDSPNAME CHECK (UPPER(ENTRY_DSP_KEY) = ENTRY_KEY) \n" + 
        ")";
    public static final String CREATE_TABLE_NODES = 
        "CREATE TABLE NODES \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL,  \n" + 
        "    NODE_NAME VARCHAR(255) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT NODES_CHKNAME CHECK (UPPER(NODE_NAME) = NODE_NAME AND LENGTH(NODE_NAME) >= 2), \n" + 
        "    NODE_DSP_NAME VARCHAR(255) NOT NULL, \n" + 
        "    NODE_FLAGS BIGINT NOT NULL, \n" + 
        "    NODE_TYPE INT NOT NULL, \n" + 
        "    OBJECT_PATH VARCHAR(512) NOT NULL, \n" + 
        "    CONSTRAINT NODES_CHKDSPNAME CHECK (UPPER(NODE_DSP_NAME) = NODE_NAME), \n" + 
        "    FOREIGN KEY (OBJECT_PATH) REFERENCES SEC_OBJECT_PROTECTION(OBJECT_PATH)  \n" + 
        ")";
    public static final String CREATE_TABLE_NODE_NET_INTERFACES = 
        "CREATE TABLE NODE_NET_INTERFACES \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL,  \n" + 
        "    NODE_NAME VARCHAR(255) NOT NULL,  \n" + 
        "    NODE_NET_NAME VARCHAR(255) NOT NULL, \n" + 
        "    NODE_NET_DSP_NAME VARCHAR(255) NOT NULL, \n" + 
        "    INET_ADDRESS VARCHAR(270) NOT NULL, \n" + 
        "    INET_TRANSPORT_TYPE VARCHAR(40) NOT NULL, \n" + 
        "    PRIMARY KEY (NODE_NAME, NODE_NET_NAME), \n" + 
        "    FOREIGN KEY (NODE_NAME) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_RESOURCE_DEFINITIONS = 
        "CREATE TABLE RESOURCE_DEFINITIONS \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL,  \n" + 
        "    RESOURCE_NAME VARCHAR(48) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT RSC_DFN_CHKNAME CHECK (UPPER(RESOURCE_NAME) = RESOURCE_NAME AND LENGTH(RESOURCE_NAME) >= 3), \n" + 
        "    RESOURCE_DSP_NAME VARCHAR(48) NOT NULL, \n" + 
        "    CONSTRAINT RSC_DFN_CHKDSPNAME CHECK (UPPER(RESOURCE_DSP_NAME) = RESOURCE_NAME) \n" + 
        ")";
    public static final String CREATE_TABLE_NODE_RESOURCE = 
        "CREATE TABLE NODE_RESOURCE \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL,  \n" + 
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" + 
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" + 
        "    NODE_ID INT NOT NULL, \n" + 
        "    RES_FLAGS BIGINT NOT NULL, \n" + 
        "    PRIMARY KEY (NODE_NAME, RESOURCE_NAME), \n" + 
        "    FOREIGN KEY (RESOURCE_NAME) REFERENCES RESOURCE_DEFINITIONS(RESOURCE_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (NODE_NAME) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_VOLUME_DEFINITIONS = 
        "CREATE TABLE VOLUME_DEFINITIONS \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL,  \n" + 
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" + 
        "    VLM_ID INT NOT NULL, \n" + 
        "    VLM_SIZE BIGINT NOT NULL, \n" + 
        "    VLM_MINOR_NR INT NOT NULL UNIQUE, \n" + 
        "    PRIMARY KEY (RESOURCE_NAME, VLM_ID), \n" + 
        "    FOREIGN KEY (RESOURCE_NAME) REFERENCES RESOURCE_DEFINITIONS(RESOURCE_NAME) ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_VOLUMES = 
        "CREATE TABLE VOLUMES \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL,  \n" + 
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" + 
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" + 
        "    VLM_ID INT NOT NULL, \n" + 
        "    BLOCK_DEVICE_PATH VARCHAR(255) NOT NULL, \n" + 
        "    VLM_FLAGS BIGINT NOT NULL, \n" + 
        "    PRIMARY KEY (NODE_NAME, RESOURCE_NAME, VLM_ID), \n" + 
        "    FOREIGN KEY (NODE_NAME) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (RESOURCE_NAME, VLM_ID) REFERENCES VOLUME_DEFINITIONS(RESOURCE_NAME, VLM_ID) ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_STOR_POOL_DEFINITIONS = 
        "CREATE TABLE STOR_POOL_DEFINITIONS \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL,  \n" + 
        "    POOL_NAME VARCHAR(32) NOT NULL PRIMARY KEY \n" + 
        "        CONSTRAINT STOR_POOL_CHKNAME CHECK (UPPER(POOL_NAME) = POOL_NAME AND LENGTH(POOL_NAME) >= 3), \n" + 
        "    POOL_DSP_NAME VARCHAR(32) NOT NULL, \n" + 
        "    CONSTRAINT STOR_POOL_CHKDSPNAME CHECK (UPPER(POOL_DSP_NAME) = POOL_NAME) \n" + 
        ")";
    public static final String CREATE_TABLE_NODE_STOR_POOL = 
        "CREATE TABLE NODE_STOR_POOL \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL UNIQUE,  \n" + 
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" + 
        "    POOL_NAME VARCHAR(32) NOT NULL, \n" + 
        "    DRIVER_NAME VARCHAR(256) NOT NULL, \n" + 
        "    PRIMARY KEY (NODE_NAME, POOL_NAME), \n" + 
        "    FOREIGN KEY (NODE_NAME) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (POOL_NAME) REFERENCES STOR_POOL_DEFINITIONS(POOL_NAME) ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_CONNECTION_DEFINITIONS = 
        "CREATE TABLE CONNECTION_DEFINITIONS \n" + 
        "( \n" + 
        "    UUID CHAR(16) FOR BIT DATA NOT NULL, \n" + 
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" + 
        "    NODE_NAME_SRC VARCHAR(255) NOT NULL, \n" + 
        "    NODE_NAME_DST VARCHAR(255) NOT NULL, \n" + 
        "    PRIMARY KEY (RESOURCE_NAME, NODE_NAME_SRC, NODE_NAME_DST), \n" + 
        "    FOREIGN KEY (RESOURCE_NAME) REFERENCES RESOURCE_DEFINITIONS(RESOURCE_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (NODE_NAME_SRC) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" + 
        "    FOREIGN KEY (NODE_NAME_DST) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE \n" + 
        ")";
    public static final String CREATE_TABLE_PROPS_CONTAINERS = 
        "CREATE TABLE PROPS_CONTAINERS \n" + 
        "( \n" + 
        "    PROPS_INSTANCE VARCHAR(512) NOT NULL \n" + 
        "        CONSTRAINT PRP_INST_CHKNAME CHECK(UPPER(PROPS_INSTANCE) = PROPS_INSTANCE AND LENGTH(PROPS_INSTANCE) >= 2), \n" + 
        "    PROP_KEY VARCHAR(512) NOT NULL, \n" + 
        "    PROP_VALUE VARCHAR(4096) NOT NULL, \n" + 
        "    PRIMARY KEY (PROPS_INSTANCE, PROP_KEY) \n" + 
        ")";

    // create views
    public static final String CREATE_VIEW_SEC_IDENTITIES_LOAD = 
        "CREATE VIEW SEC_IDENTITIES_LOAD AS \n" + 
        "    SELECT IDENTITY_DSP_NAME, ID_ENABLED \n" + 
        "    FROM SEC_IDENTITIES";
    public static final String CREATE_VIEW_SEC_ROLES_LOAD = 
        "CREATE VIEW SEC_ROLES_LOAD AS \n" + 
        "    SELECT ROLE_DSP_NAME, ROLE_ENABLED \n" + 
        "    FROM SEC_ROLES";
    public static final String CREATE_VIEW_SEC_TYPES_LOAD = 
        "CREATE VIEW SEC_TYPES_LOAD AS \n" + 
        "    SELECT TYPE_DSP_NAME, TYPE_ENABLED \n" + 
        "    FROM SEC_TYPES";
    public static final String CREATE_VIEW_SEC_TYPE_RULES_LOAD = 
        "CREATE VIEW SEC_TYPE_RULES_LOAD AS \n" + 
        "    SELECT DOMAIN_NAME, TYPE_NAME, SEC_ACCESS_TYPES.ACCESS_TYPE_NAME AS ACCESS_TYPE \n" + 
        "    FROM SEC_TYPE_RULES \n" + 
        "    LEFT JOIN SEC_ACCESS_TYPES ON SEC_TYPE_RULES.ACCESS_TYPE = SEC_ACCESS_TYPES.ACCESS_TYPE_VALUE \n" + 
        "    ORDER BY DOMAIN_NAME, TYPE_NAME ASC";

    // drop statements
    public static final String DROP_VIEW_SEC_TYPE_RULES_LOAD   = "DROP VIEW " + VIEW_SEC_TYPE_RULES_LOAD;
    public static final String DROP_VIEW_SEC_TYPES_LOAD        = "DROP VIEW " + VIEW_SEC_TYPES_LOAD;
    public static final String DROP_VIEW_SEC_ROLES_LOAD        = "DROP VIEW " + VIEW_SEC_ROLES_LOAD;
    public static final String DROP_VIEW_SEC_IDENTITIES_LOAD   = "DROP VIEW " + VIEW_SEC_IDENTITIES_LOAD;
    public static final String DROP_TBL_PROPS_CONTAINERS       = "DROP TABLE " + TBL_PROPS_CONTAINERS;
    public static final String DROP_TBL_CONNECTION_DEFINITIONS = "DROP TABLE " + TBL_CONNECTION_DEFINITIONS;
    public static final String DROP_TBL_NODE_STOR_POOL         = "DROP TABLE " + TBL_NODE_STOR_POOL;
    public static final String DROP_TBL_STOR_POOL_DEFINITIONS  = "DROP TABLE " + TBL_STOR_POOL_DEFINITIONS;
    public static final String DROP_TBL_VOLUMES                = "DROP TABLE " + TBL_VOLUMES;
    public static final String DROP_TBL_VOLUME_DEFINITIONS     = "DROP TABLE " + TBL_VOLUME_DEFINITIONS;
    public static final String DROP_TBL_NODE_RESOURCE          = "DROP TABLE " + TBL_NODE_RESOURCE;
    public static final String DROP_TBL_RESOURCE_DEFINITIONS   = "DROP TABLE " + TBL_RESOURCE_DEFINITIONS;
    public static final String DROP_TBL_NODE_NET_INTERFACES    = "DROP TABLE " + TBL_NODE_NET_INTERFACES;
    public static final String DROP_TBL_NODES                  = "DROP TABLE " + TBL_NODES;
    public static final String DROP_TBL_CTRL_CONFIGURATION     = "DROP TABLE " + TBL_CTRL_CONFIGURATION;
    public static final String DROP_TBL_SEC_ACL_MAP            = "DROP TABLE " + TBL_SEC_ACL_MAP;
    public static final String DROP_TBL_SEC_OBJECT_PROTECTION  = "DROP TABLE " + TBL_SEC_OBJECT_PROTECTION;
    public static final String DROP_TBL_SEC_DFLT_ROLES         = "DROP TABLE " + TBL_SEC_DFLT_ROLES;
    public static final String DROP_TBL_SEC_TYPE_RULES         = "DROP TABLE " + TBL_SEC_TYPE_RULES;
    public static final String DROP_TBL_SEC_ACCESS_TYPES       = "DROP TABLE " + TBL_SEC_ACCESS_TYPES;
    public static final String DROP_TBL_SEC_ID_ROLE_MAP        = "DROP TABLE " + TBL_SEC_ID_ROLE_MAP;
    public static final String DROP_TBL_SEC_ROLES              = "DROP TABLE " + TBL_SEC_ROLES;
    public static final String DROP_TBL_SEC_TYPES              = "DROP TABLE " + TBL_SEC_TYPES;
    public static final String DROP_TBL_SEC_IDENTITIES         = "DROP TABLE " + TBL_SEC_IDENTITIES;
    public static final String DROP_TBL_SEC_CONFIGURATION      = "DROP TABLE " + TBL_SEC_CONFIGURATION;

    // truncate statements
    public static final String TRUNCATE_PROPS_CONTAINERS       = "DELETE FROM " + TBL_PROPS_CONTAINERS;
    public static final String TRUNCATE_CONNECTION_DEFINITIONS = "DELETE FROM " + TBL_CONNECTION_DEFINITIONS;
    public static final String TRUNCATE_NODE_STOR_POOL         = "DELETE FROM " + TBL_NODE_STOR_POOL;
    public static final String TRUNCATE_STOR_POOL_DEFINITIONS  = "DELETE FROM " + TBL_STOR_POOL_DEFINITIONS;
    public static final String TRUNCATE_VOLUMES                = "DELETE FROM " + TBL_VOLUMES;
    public static final String TRUNCATE_VOLUME_DEFINITIONS     = "DELETE FROM " + TBL_VOLUME_DEFINITIONS;
    public static final String TRUNCATE_NODE_RESOURCE          = "DELETE FROM " + TBL_NODE_RESOURCE;
    public static final String TRUNCATE_RESOURCE_DEFINITIONS   = "DELETE FROM " + TBL_RESOURCE_DEFINITIONS;
    public static final String TRUNCATE_NODE_NET_INTERFACES    = "DELETE FROM " + TBL_NODE_NET_INTERFACES;
    public static final String TRUNCATE_NODES                  = "DELETE FROM " + TBL_NODES;
    public static final String TRUNCATE_CTRL_CONFIGURATION     = "DELETE FROM " + TBL_CTRL_CONFIGURATION;
    public static final String TRUNCATE_SEC_ACL_MAP            = "DELETE FROM " + TBL_SEC_ACL_MAP;
    public static final String TRUNCATE_SEC_OBJECT_PROTECTION  = "DELETE FROM " + TBL_SEC_OBJECT_PROTECTION;
    public static final String TRUNCATE_SEC_DFLT_ROLES         = "DELETE FROM " + TBL_SEC_DFLT_ROLES;
    public static final String TRUNCATE_SEC_TYPE_RULES         = "DELETE FROM " + TBL_SEC_TYPE_RULES;
    public static final String TRUNCATE_SEC_ACCESS_TYPES       = "DELETE FROM " + TBL_SEC_ACCESS_TYPES;
    public static final String TRUNCATE_SEC_ID_ROLE_MAP        = "DELETE FROM " + TBL_SEC_ID_ROLE_MAP;
    public static final String TRUNCATE_SEC_ROLES              = "DELETE FROM " + TBL_SEC_ROLES;
    public static final String TRUNCATE_SEC_TYPES              = "DELETE FROM " + TBL_SEC_TYPES;
    public static final String TRUNCATE_SEC_IDENTITIES         = "DELETE FROM " + TBL_SEC_IDENTITIES;
    public static final String TRUNCATE_SEC_CONFIGURATION      = "DELETE FROM " + TBL_SEC_CONFIGURATION;

    // create statement array
    public static final String[] CREATE_TABLES = 
    {
        CREATE_TABLE_SEC_CONFIGURATION,
        CREATE_TABLE_SEC_IDENTITIES,
        CREATE_TABLE_SEC_TYPES,
        CREATE_TABLE_SEC_ROLES,
        CREATE_TABLE_SEC_ID_ROLE_MAP,
        CREATE_TABLE_SEC_ACCESS_TYPES,
        CREATE_TABLE_SEC_TYPE_RULES,
        CREATE_TABLE_SEC_DFLT_ROLES,
        CREATE_TABLE_SEC_OBJECT_PROTECTION,
        CREATE_TABLE_SEC_ACL_MAP,
        CREATE_TABLE_CTRL_CONFIGURATION,
        CREATE_TABLE_NODES,
        CREATE_TABLE_NODE_NET_INTERFACES,
        CREATE_TABLE_RESOURCE_DEFINITIONS,
        CREATE_TABLE_NODE_RESOURCE,
        CREATE_TABLE_VOLUME_DEFINITIONS,
        CREATE_TABLE_VOLUMES,
        CREATE_TABLE_STOR_POOL_DEFINITIONS,
        CREATE_TABLE_NODE_STOR_POOL,
        CREATE_TABLE_CONNECTION_DEFINITIONS,
        CREATE_TABLE_PROPS_CONTAINERS,
        CREATE_VIEW_SEC_IDENTITIES_LOAD,
        CREATE_VIEW_SEC_ROLES_LOAD,
        CREATE_VIEW_SEC_TYPES_LOAD,
        CREATE_VIEW_SEC_TYPE_RULES_LOAD,
    };

    // insert statements (default values)
    public static final String[] INSERT_DEFAULT_VALUES = 
    {
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" + 
        "    VALUES ('CONTROL', 15)",
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" + 
        "    VALUES ('CHANGE', 7)",
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" + 
        "    VALUES ('USE', 3)",
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" + 
        "    VALUES ('VIEW', 1)",
        "INSERT INTO SEC_IDENTITIES (IDENTITY_NAME, IDENTITY_DSP_NAME, ID_ENABLED, ID_LOCKED) \n" + 
        "    VALUES('SYSTEM', 'SYSTEM', TRUE, TRUE)",
        "INSERT INTO SEC_IDENTITIES (IDENTITY_NAME, IDENTITY_DSP_NAME, ID_ENABLED, ID_LOCKED) \n" + 
        "    VALUES('PUBLIC', 'PUBLIC', TRUE, TRUE)",
        "INSERT INTO SEC_TYPES (TYPE_NAME, TYPE_DSP_NAME, TYPE_ENABLED) \n" + 
        "    VALUES ('SYSTEM', 'SYSTEM', TRUE)",
        "INSERT INTO SEC_TYPES (TYPE_NAME, TYPE_DSP_NAME, TYPE_ENABLED) \n" + 
        "    VALUES ('PUBLIC', 'PUBLIC', TRUE)",
        "INSERT INTO SEC_ROLES (ROLE_NAME, ROLE_DSP_NAME, DOMAIN_NAME, ROLE_ENABLED, ROLE_PRIVILEGES) \n" + 
        "    VALUES('SYSTEM', 'SYSTEM', 'SYSTEM', TRUE, -9223372036854775808)",
        "INSERT INTO SEC_ROLES (ROLE_NAME, ROLE_DSP_NAME, DOMAIN_NAME, ROLE_ENABLED, ROLE_PRIVILEGES) \n" + 
        "    VALUES('PUBLIC', 'PUBLIC', 'PUBLIC', TRUE, 0)",
        "INSERT INTO SEC_ID_ROLE_MAP (IDENTITY_NAME, ROLE_NAME) \n" + 
        "    VALUES ('SYSTEM', 'SYSTEM')",
        "INSERT INTO SEC_ID_ROLE_MAP (IDENTITY_NAME, ROLE_NAME) \n" + 
        "    VALUES ('PUBLIC', 'PUBLIC')",
        "INSERT INTO SEC_DFLT_ROLES (IDENTITY_NAME, ROLE_NAME) \n" + 
        "    VALUES ('SYSTEM', 'SYSTEM')",
        "INSERT INTO SEC_DFLT_ROLES (IDENTITY_NAME, ROLE_NAME) \n" + 
        "    VALUES ('PUBLIC', 'PUBLIC')",
        "INSERT INTO SEC_CONFIGURATION (ENTRY_KEY, ENTRY_DSP_KEY, ENTRY_VALUE) \n" + 
        "    VALUES ('SECURITYLEVEL', 'SecurityLevel', 'MAC')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/tcp0/bindaddress', 'localhost')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/tcp0/port', '9500')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/tcp0/type', 'plain')",
    };

    // insert statements (parameterized)
    public static final String INSERT_SEC_CONFIGURATION = 
        " INSERT INTO " + TBL_SEC_CONFIGURATION + 
        " VALUES (?, ?, ?)";
    public static final String INSERT_SEC_IDENTITIES = 
        " INSERT INTO " + TBL_SEC_IDENTITIES + 
        " VALUES (?, ?, ?, ?, ?, ?)";
    public static final String INSERT_SEC_TYPES = 
        " INSERT INTO " + TBL_SEC_TYPES + 
        " VALUES (?, ?, ?)";
    public static final String INSERT_SEC_ROLES = 
        " INSERT INTO " + TBL_SEC_ROLES + 
        " VALUES (?, ?, ?, ?, ?)";
    public static final String INSERT_SEC_ID_ROLE_MAP = 
        " INSERT INTO " + TBL_SEC_ID_ROLE_MAP + 
        " VALUES (?, ?)";
    public static final String INSERT_SEC_ACCESS_TYPES = 
        " INSERT INTO " + TBL_SEC_ACCESS_TYPES + 
        " VALUES (?, ?)";
    public static final String INSERT_SEC_TYPE_RULES = 
        " INSERT INTO " + TBL_SEC_TYPE_RULES + 
        " VALUES (?, ?, ?)";
    public static final String INSERT_SEC_DFLT_ROLES = 
        " INSERT INTO " + TBL_SEC_DFLT_ROLES + 
        " VALUES (?, ?)";
    public static final String INSERT_SEC_OBJECT_PROTECTION = 
        " INSERT INTO " + TBL_SEC_OBJECT_PROTECTION + 
        " VALUES (?, ?, ?, ?)";
    public static final String INSERT_SEC_ACL_MAP = 
        " INSERT INTO " + TBL_SEC_ACL_MAP + 
        " VALUES (?, ?, ?)";
    public static final String INSERT_CTRL_CONFIGURATION = 
        " INSERT INTO " + TBL_CTRL_CONFIGURATION + 
        " VALUES (?, ?, ?)";
    public static final String INSERT_NODES = 
        " INSERT INTO " + TBL_NODES + 
        " VALUES (?, ?, ?, ?, ?, ?)";
    public static final String INSERT_NODE_NET_INTERFACES = 
        " INSERT INTO " + TBL_NODE_NET_INTERFACES + 
        " VALUES (?, ?, ?, ?, ?, ?)";
    public static final String INSERT_RESOURCE_DEFINITIONS = 
        " INSERT INTO " + TBL_RESOURCE_DEFINITIONS + 
        " VALUES (?, ?, ?)";
    public static final String INSERT_NODE_RESOURCE = 
        " INSERT INTO " + TBL_NODE_RESOURCE + 
        " VALUES (?, ?, ?, ?, ?)";
    public static final String INSERT_VOLUME_DEFINITIONS = 
        " INSERT INTO " + TBL_VOLUME_DEFINITIONS + 
        " VALUES (?, ?, ?, ?, ?)";
    public static final String INSERT_VOLUMES = 
        " INSERT INTO " + TBL_VOLUMES + 
        " VALUES (?, ?, ?, ?, ?, ?)";
    public static final String INSERT_STOR_POOL_DEFINITIONS = 
        " INSERT INTO " + TBL_STOR_POOL_DEFINITIONS + 
        " VALUES (?, ?, ?)";
    public static final String INSERT_NODE_STOR_POOL = 
        " INSERT INTO " + TBL_NODE_STOR_POOL + 
        " VALUES (?, ?, ?, ?)";
    public static final String INSERT_CONNECTION_DEFINITIONS = 
        " INSERT INTO " + TBL_CONNECTION_DEFINITIONS + 
        " VALUES (?, ?, ?, ?)";
    public static final String INSERT_PROPS_CONTAINERS = 
        " INSERT INTO " + TBL_PROPS_CONTAINERS + 
        " VALUES (?, ?, ?)";

    // drop statement array
    public static final String[] DROP_TABLES = 
    {
        DROP_VIEW_SEC_TYPE_RULES_LOAD,
        DROP_VIEW_SEC_TYPES_LOAD,
        DROP_VIEW_SEC_ROLES_LOAD,
        DROP_VIEW_SEC_IDENTITIES_LOAD,
        DROP_TBL_PROPS_CONTAINERS,
        DROP_TBL_CONNECTION_DEFINITIONS,
        DROP_TBL_NODE_STOR_POOL,
        DROP_TBL_STOR_POOL_DEFINITIONS,
        DROP_TBL_VOLUMES,
        DROP_TBL_VOLUME_DEFINITIONS,
        DROP_TBL_NODE_RESOURCE,
        DROP_TBL_RESOURCE_DEFINITIONS,
        DROP_TBL_NODE_NET_INTERFACES,
        DROP_TBL_NODES,
        DROP_TBL_CTRL_CONFIGURATION,
        DROP_TBL_SEC_ACL_MAP,
        DROP_TBL_SEC_OBJECT_PROTECTION,
        DROP_TBL_SEC_DFLT_ROLES,
        DROP_TBL_SEC_TYPE_RULES,
        DROP_TBL_SEC_ACCESS_TYPES,
        DROP_TBL_SEC_ID_ROLE_MAP,
        DROP_TBL_SEC_ROLES,
        DROP_TBL_SEC_TYPES,
        DROP_TBL_SEC_IDENTITIES,
        DROP_TBL_SEC_CONFIGURATION,
    };

    // truncate statement array
    public static final String[] TRUNCATE_TABLES = 
    {
        TRUNCATE_PROPS_CONTAINERS,
        TRUNCATE_CONNECTION_DEFINITIONS,
        TRUNCATE_NODE_STOR_POOL,
        TRUNCATE_STOR_POOL_DEFINITIONS,
        TRUNCATE_VOLUMES,
        TRUNCATE_VOLUME_DEFINITIONS,
        TRUNCATE_NODE_RESOURCE,
        TRUNCATE_RESOURCE_DEFINITIONS,
        TRUNCATE_NODE_NET_INTERFACES,
        TRUNCATE_NODES,
        TRUNCATE_CTRL_CONFIGURATION,
        TRUNCATE_SEC_ACL_MAP,
        TRUNCATE_SEC_OBJECT_PROTECTION,
        TRUNCATE_SEC_DFLT_ROLES,
        TRUNCATE_SEC_TYPE_RULES,
        TRUNCATE_SEC_ACCESS_TYPES,
        TRUNCATE_SEC_ID_ROLE_MAP,
        TRUNCATE_SEC_ROLES,
        TRUNCATE_SEC_TYPES,
        TRUNCATE_SEC_IDENTITIES,
        TRUNCATE_SEC_CONFIGURATION,
    };
}
