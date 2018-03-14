package com.linbit.linstor.security;

public interface DerbyTestConstants
{
    // View names
    String VIEW_SEC_IDENTITIES_LOAD = "SEC_IDENTITIES_LOAD";
    String VIEW_SEC_ROLES_LOAD      = "SEC_ROLES_LOAD";
    String VIEW_SEC_TYPES_LOAD      = "SEC_TYPES_LOAD";
    String VIEW_SEC_TYPE_RULES_LOAD = "SEC_TYPE_RULES_LOAD";

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
    public static final String NODE_NET_NAME     = "NODE_NET_NAME";
    public static final String NODE_NET_DSP_NAME = "NODE_NET_DSP_NAME";
    public static final String INET_ADDRESS      = "INET_ADDRESS";

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
    public static final String BLOCK_DEVICE_PATH = "BLOCK_DEVICE_PATH";
    public static final String META_DISK_PATH    = "META_DISK_PATH";

    // NODE_CONNECTIONS column names
    public static final String NODE_NAME_SRC = "NODE_NAME_SRC";
    public static final String NODE_NAME_DST = "NODE_NAME_DST";

    // PROPS_CONTAINERS column names
    public static final String PROPS_INSTANCE = "PROPS_INSTANCE";
    public static final String PROP_KEY       = "PROP_KEY";
    public static final String PROP_VALUE     = "PROP_VALUE";

    // table column counts
    public static final int TBL_COL_COUNT_SEC_CONFIGURATION     = 3;
    public static final int TBL_COL_COUNT_SEC_IDENTITIES        = 6;
    public static final int TBL_COL_COUNT_SEC_TYPES             = 3;
    public static final int TBL_COL_COUNT_SEC_ROLES             = 5;
    public static final int TBL_COL_COUNT_SEC_ID_ROLE_MAP       = 2;
    public static final int TBL_COL_COUNT_SEC_ACCESS_TYPES      = 2;
    public static final int TBL_COL_COUNT_SEC_TYPE_RULES        = 3;
    public static final int TBL_COL_COUNT_SEC_DFLT_ROLES        = 2;
    public static final int TBL_COL_COUNT_SEC_OBJECT_PROTECTION = 4;
    public static final int TBL_COL_COUNT_SEC_ACL_MAP           = 3;
    public static final int TBL_COL_COUNT_NODES                 = 5;
    public static final int TBL_COL_COUNT_NODE_NET_INTERFACES   = 5;
    public static final int TBL_COL_COUNT_SATELLITE_CONNECTIONS = 5;
    public static final int TBL_COL_COUNT_RESOURCE_DEFINITIONS  = 7;
    public static final int TBL_COL_COUNT_RESOURCES             = 5;
    public static final int TBL_COL_COUNT_STOR_POOL_DEFINITIONS = 3;
    public static final int TBL_COL_COUNT_NODE_STOR_POOL        = 4;
    public static final int TBL_COL_COUNT_VOLUME_DEFINITIONS    = 6;
    public static final int TBL_COL_COUNT_VOLUMES               = 8;
    public static final int TBL_COL_COUNT_NODE_CONNECTIONS      = 3;
    public static final int TBL_COL_COUNT_RESOURCE_CONNECTIONS  = 4;
    public static final int TBL_COL_COUNT_VOLUME_CONNECTIONS    = 5;
    public static final int TBL_COL_COUNT_PROPS_CONTAINERS      = 3;

    // create statements
    public static final String CREATE_TABLE_SEC_CONFIGURATION =
        "CREATE TABLE SEC_CONFIGURATION \n" +
        "( \n" +
        "    ENTRY_KEY VARCHAR(24) NOT NULL, \n" +
        "    ENTRY_DSP_KEY VARCHAR(24) NOT NULL, \n" +
        "    ENTRY_VALUE VARCHAR(24) NOT NULL, \n" +
        "    CONSTRAINT PK_SCFG PRIMARY KEY (ENTRY_KEY), \n" +
        "    CONSTRAINT CHK_SCFG_KEY CHECK (UPPER(ENTRY_KEY) = ENTRY_KEY AND LENGTH(ENTRY_KEY) >= 3), \n" +
        "    CONSTRAINT CHK_SCFG_DSP_KEY CHECK (UPPER(ENTRY_DSP_KEY) = ENTRY_KEY) \n" +
        ")";
    public static final String CREATE_TABLE_SEC_IDENTITIES =
        "CREATE TABLE SEC_IDENTITIES \n" +
        "( \n" +
        "    IDENTITY_NAME VARCHAR(24) NOT NULL, \n" +
        "    IDENTITY_DSP_NAME VARCHAR(24) NOT NULL, \n" +
        "    PASS_SALT CHAR(32), \n" +
        "    PASS_HASH CHAR(128), \n" +
        "    ID_ENABLED BOOLEAN DEFAULT TRUE NOT NULL, \n" +
        "    ID_LOCKED BOOLEAN DEFAULT TRUE NOT NULL, \n" +
        "    CONSTRAINT PK_SI PRIMARY KEY (IDENTITY_NAME), \n" +
        "    CONSTRAINT CHK_SI_NAME CHECK (UPPER(IDENTITY_NAME) = IDENTITY_NAME AND LENGTH(IDENTITY_NAME) >= 3), \n" +
        "    CONSTRAINT CHK_SI_DSP_NAME CHECK (UPPER(IDENTITY_DSP_NAME) = IDENTITY_NAME) \n" +
        ")";
    public static final String CREATE_TABLE_SEC_TYPES =
        "CREATE TABLE SEC_TYPES \n" +
        "( \n" +
        "    TYPE_NAME VARCHAR(24) NOT NULL, \n" +
        "    TYPE_DSP_NAME VARCHAR(24) NOT NULL, \n" +
        "    TYPE_ENABLED BOOLEAN DEFAULT TRUE NOT NULL, \n" +
        "    CONSTRAINT PK_ST PRIMARY KEY (TYPE_NAME), \n" +
        "    CONSTRAINT CHK_ST_NAME CHECK (UPPER(TYPE_NAME) = TYPE_NAME AND LENGTH(TYPE_NAME) >= 3), \n" +
        "    CONSTRAINT CHK_ST_DSPNAME CHECK (UPPER(TYPE_DSP_NAME) = TYPE_NAME) \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ROLES =
        "CREATE TABLE SEC_ROLES \n" +
        "( \n" +
        "    ROLE_NAME VARCHAR(24) NOT NULL, \n" +
        "    ROLE_DSP_NAME VARCHAR(24) NOT NULL, \n" +
        "    DOMAIN_NAME VARCHAR(24) NOT NULL, \n" +
        "    ROLE_ENABLED BOOLEAN DEFAULT TRUE NOT NULL, \n" +
        "    ROLE_PRIVILEGES BIGINT DEFAULT 0 NOT NULL, \n" +
        "    CONSTRAINT PK_SR PRIMARY KEY (ROLE_NAME), \n" +
        "    CONSTRAINT FK_SR_SEC_TYPE FOREIGN KEY (DOMAIN_NAME) REFERENCES SEC_TYPES(TYPE_NAME), \n" +
        "    CONSTRAINT CHK_SR_ROLE_NAME CHECK (UPPER(ROLE_NAME) = ROLE_NAME AND LENGTH(ROLE_NAME) >= 3), \n" +
        "    CONSTRAINT CHK_SR_ROLE_DSP_NAME CHECK (UPPER(ROLE_DSP_NAME) = ROLE_NAME) \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ID_ROLE_MAP =
        "CREATE TABLE SEC_ID_ROLE_MAP \n" +
        "( \n" +
        "    IDENTITY_NAME VARCHAR(24) NOT NULL, \n" +
        "    ROLE_NAME VARCHAR(24) NOT NULL, \n" +
        "    CONSTRAINT PK_SIRM PRIMARY KEY (IDENTITY_NAME, ROLE_NAME), \n" +
        "    CONSTRAINT FK_SIRM_SEC_ID FOREIGN KEY (IDENTITY_NAME) REFERENCES SEC_IDENTITIES(IDENTITY_NAME) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_SIRM_SEC_ROLE FOREIGN KEY (ROLE_NAME) REFERENCES SEC_ROLES(ROLE_NAME) ON DELETE CASCADE \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ACCESS_TYPES =
        "CREATE TABLE SEC_ACCESS_TYPES \n" +
        "( \n" +
        "    ACCESS_TYPE_NAME VARCHAR(24) NOT NULL, \n" +
        "    ACCESS_TYPE_VALUE SMALLINT NOT NULL, \n" +
        "    CONSTRAINT PK_SAT PRIMARY KEY (ACCESS_TYPE_NAME), \n" +
        "    CONSTRAINT UNQ_SAT_TYPE_VALUE UNIQUE (ACCESS_TYPE_VALUE), \n" +
        "    CONSTRAINT CHK_SAT_TYPE_NAME CHECK (UPPER(ACCESS_TYPE_NAME) = ACCESS_TYPE_NAME) \n" +
        ")";
    public static final String CREATE_TABLE_SEC_TYPE_RULES =
        "CREATE TABLE SEC_TYPE_RULES \n" +
        "( \n" +
        "    DOMAIN_NAME VARCHAR(24) NOT NULL, \n" +
        "    TYPE_NAME VARCHAR(24) NOT NULL, \n" +
        "    ACCESS_TYPE SMALLINT NOT NULL, \n" +
        "    CONSTRAINT PK_STR PRIMARY KEY (DOMAIN_NAME, TYPE_NAME), \n" +
        "    CONSTRAINT FK_STR_SEC_TYPE_DOMAIN FOREIGN KEY (DOMAIN_NAME) REFERENCES SEC_TYPES(TYPE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_STR_SEC_TYPE_TYPE   FOREIGN KEY (TYPE_NAME) REFERENCES SEC_TYPES(TYPE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_STR_ACCESS_TYPE     FOREIGN KEY (ACCESS_TYPE) REFERENCES SEC_ACCESS_TYPES(ACCESS_TYPE_VALUE) \n" +
        "        ON DELETE RESTRICT \n" +
        ")";
    public static final String CREATE_TABLE_SEC_DFLT_ROLES =
        "CREATE TABLE SEC_DFLT_ROLES \n" +
        "( \n" +
        "    IDENTITY_NAME VARCHAR(24) NOT NULL, \n" +
        "    ROLE_NAME VARCHAR(24) NOT NULL, \n" +
        "    CONSTRAINT PK_SDR PRIMARY KEY (IDENTITY_NAME), \n" +
        "    CONSTRAINT FK_SDR_SEC_ID_ROLE_MAP FOREIGN KEY (IDENTITY_NAME, ROLE_NAME) REFERENCES \n" +
        "        SEC_ID_ROLE_MAP(IDENTITY_NAME, ROLE_NAME) ON DELETE CASCADE \n" +
        ")";
    public static final String CREATE_TABLE_SEC_OBJECT_PROTECTION =
        "CREATE TABLE SEC_OBJECT_PROTECTION \n" +
        "( \n" +
        "    OBJECT_PATH VARCHAR(512) NOT NULL, \n" +
        "    CREATOR_IDENTITY_NAME VARCHAR(24) NOT NULL, \n" +
        "    OWNER_ROLE_NAME VARCHAR(24) NOT NULL, \n" +
        "    SECURITY_TYPE_NAME VARCHAR(24) NOT NULL, \n" +
        "    CONSTRAINT PK_SOP PRIMARY KEY (OBJECT_PATH), \n" +
        "    CONSTRAINT FK_SOP_SEC_ID FOREIGN KEY (CREATOR_IDENTITY_NAME) REFERENCES SEC_IDENTITIES(IDENTITY_NAME) \n" +
        "        ON DELETE RESTRICT, \n" +
        "    CONSTRAINT FK_SOP_SEC_ROLES FOREIGN KEY (OWNER_ROLE_NAME) REFERENCES SEC_ROLES(ROLE_NAME) \n" +
        "        ON DELETE RESTRICT, \n" +
        "    CONSTRAINT FK_SOP_SEC_TYPES FOREIGN KEY (SECURITY_TYPE_NAME) REFERENCES SEC_TYPES(TYPE_NAME) \n" +
        "        ON DELETE RESTRICT \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ACL_MAP =
        "CREATE TABLE SEC_ACL_MAP \n" +
        "( \n" +
        "    OBJECT_PATH VARCHAR(512) NOT NULL, \n" +
        "    ROLE_NAME VARCHAR(24) NOT NULL, \n" +
        "    ACCESS_TYPE SMALLINT NOT NULL, \n" +
        "    CONSTRAINT PK_SAM PRIMARY KEY (OBJECT_PATH, ROLE_NAME), \n" +
        "    CONSTRAINT FK_SAM_SEC_OBJ_PROT FOREIGN KEY (OBJECT_PATH) REFERENCES SEC_OBJECT_PROTECTION(OBJECT_PATH) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_SAM_SEC_ROLES FOREIGN KEY (ROLE_NAME) REFERENCES SEC_ROLES(ROLE_NAME) \n" +
        "        ON DELETE RESTRICT, \n" +
        "    CONSTRAINT FK_SAM_SEC_ACC_TYPE FOREIGN KEY (ACCESS_TYPE) REFERENCES SEC_ACCESS_TYPES(ACCESS_TYPE_VALUE) \n" +
        "        ON DELETE RESTRICT \n" +
        ")";
    public static final String CREATE_TABLE_NODES =
        "CREATE TABLE NODES \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" +
        "    NODE_DSP_NAME VARCHAR(255) NOT NULL, \n" +
        "    NODE_FLAGS BIGINT NOT NULL, \n" +
        "    NODE_TYPE INT NOT NULL, \n" +
        "    CONSTRAINT PK_N PRIMARY KEY (NODE_NAME), \n" +
        "    CONSTRAINT UNQ_N_UUID UNIQUE (UUID), \n" +
        "    CONSTRAINT CHK_N_NODES_NAME CHECK (UPPER(NODE_NAME) = NODE_NAME AND LENGTH(NODE_NAME) >= 2), \n" +
        "    CONSTRAINT CHK_N_NODES_DSPNAME CHECK (UPPER(NODE_DSP_NAME) = NODE_NAME) \n" +
        ")";
    public static final String CREATE_TABLE_NODE_NET_INTERFACES =
        "CREATE TABLE NODE_NET_INTERFACES \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" +
        "    NODE_NET_NAME VARCHAR(255) NOT NULL, \n" +
        "    NODE_NET_DSP_NAME VARCHAR(255) NOT NULL, \n" +
        "    INET_ADDRESS VARCHAR(45) NOT NULL, \n" +
        "    CONSTRAINT PK_NNI PRIMARY KEY (NODE_NAME, NODE_NET_NAME), \n" +
        "    CONSTRAINT FK_NNI_NODES FOREIGN KEY (NODE_NAME) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_NNI_UUID UNIQUE (UUID) \n" +
        ")";
    public static final String CREATE_TABLE_SATELLITE_CONNECTIONS =
        "CREATE TABLE SATELLITE_CONNECTIONS \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" +
        "    NODE_NET_NAME VARCHAR(255) NOT NULL, \n" +
        "    TCP_PORT SMALLINT NOT NULL, \n" +
        "    INET_TYPE VARCHAR(5) NOT NULL, \n" +
        "    CONSTRAINT PK_SCONN PRIMARY KEY (NODE_NAME), \n" +
        "    CONSTRAINT FK_SCONN_NODE_NET_IFS FOREIGN KEY (NODE_NAME, NODE_NET_NAME) REFERENCES \n" +
        "        NODE_NET_INTERFACES(NODE_NAME, NODE_NET_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_SCONN_UUID UNIQUE (UUID), \n" +
        "    CONSTRAINT CHK_SCONN_PORT_RANGE CHECK (TCP_PORT > 0 AND TCP_PORT < 65536), \n" +
        "    CONSTRAINT CHK_SCONN_TYPE CHECK (INET_TYPE = 'PLAIN' OR INET_TYPE = 'SSL') \n" +
        ")";
    public static final String CREATE_TABLE_RESOURCE_DEFINITIONS =
        "CREATE TABLE RESOURCE_DEFINITIONS \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" +
        "    RESOURCE_DSP_NAME VARCHAR(48) NOT NULL, \n" +
        "    TCP_PORT INTEGER NOT NULL, \n" +
        "    RESOURCE_FLAGS BIGINT NOT NULL, \n" +
        "    SECRET VARCHAR(20) NOT NULL, \n" +
        "    TRANSPORT_TYPE VARCHAR(40) NOT NULL, \n" +
        "    CONSTRAINT PK_RD PRIMARY KEY (RESOURCE_NAME), \n" +
        "    CONSTRAINT UNQ_RD_UUID UNIQUE (UUID), \n" +
        "    CONSTRAINT UNQ_TCP_PORT UNIQUE (TCP_PORT), \n" +
        "    CONSTRAINT CHK_RD_NAME CHECK (UPPER(RESOURCE_NAME) = RESOURCE_NAME AND LENGTH(RESOURCE_NAME) >= 3), \n" +
        "    CONSTRAINT CHK_RD_DSP_NAME CHECK (UPPER(RESOURCE_DSP_NAME) = RESOURCE_NAME), \n" +
        "    CONSTRAINT CHK_RD_PORT_RANGE CHECK (TCP_PORT > 0 AND TCP_PORT < 65536), \n" +
        "    CONSTRAINT CHK_RD_TRANSPORT_TYPE CHECK \n" +
        "        (TRANSPORT_TYPE = 'IP' OR TRANSPORT_TYPE = 'RDMA' OR TRANSPORT_TYPE = 'RoCE') \n" +
        ")";
    public static final String CREATE_TABLE_RESOURCES =
        "CREATE TABLE RESOURCES \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" +
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" +
        "    NODE_ID INT NOT NULL, \n" +
        "    RESOURCE_FLAGS BIGINT NOT NULL, \n" +
        "    CONSTRAINT PK_R PRIMARY KEY (NODE_NAME, RESOURCE_NAME), \n" +
        "    CONSTRAINT FK_R_NODES FOREIGN KEY (NODE_NAME) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_R_RSC_DFNS FOREIGN KEY (RESOURCE_NAME) REFERENCES RESOURCE_DEFINITIONS(RESOURCE_NAME) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_R_UUID UNIQUE (UUID) \n" +
        ")";
    public static final String CREATE_TABLE_STOR_POOL_DEFINITIONS =
        "CREATE TABLE STOR_POOL_DEFINITIONS \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    POOL_NAME VARCHAR(32) NOT NULL, \n" +
        "    POOL_DSP_NAME VARCHAR(32) NOT NULL, \n" +
        "    CONSTRAINT PK_SPD PRIMARY KEY (POOL_NAME), \n" +
        "    CONSTRAINT UNQ_SPD_UUID UNIQUE (UUID), \n" +
        "    CONSTRAINT CHK_SPD_NAME CHECK (UPPER(POOL_NAME) = POOL_NAME AND LENGTH(POOL_NAME) >= 3), \n" +
        "    CONSTRAINT CHK_SPD_DSP_NAME CHECK (UPPER(POOL_DSP_NAME) = POOL_NAME) \n" +
        ")";
    public static final String CREATE_TABLE_NODE_STOR_POOL =
        "CREATE TABLE NODE_STOR_POOL \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" +
        "    POOL_NAME VARCHAR(32) NOT NULL, \n" +
        "    DRIVER_NAME VARCHAR(256) NOT NULL, \n" +
        "    CONSTRAINT PK_SP PRIMARY KEY (NODE_NAME, POOL_NAME), \n" +
        "    CONSTRAINT FK_SP_NODES FOREIGN KEY (NODE_NAME) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_SP_STOR_POOL_DFNS FOREIGN KEY (POOL_NAME) REFERENCES STOR_POOL_DEFINITIONS(POOL_NAME) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_SP_UUID UNIQUE (UUID) \n" +
        ")";
    public static final String CREATE_TABLE_VOLUME_DEFINITIONS =
        "CREATE TABLE VOLUME_DEFINITIONS \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" +
        "    VLM_NR INT NOT NULL, \n" +
        "    VLM_SIZE BIGINT NOT NULL, \n" +
        "    VLM_MINOR_NR INT NOT NULL, \n" +
        "    VLM_FLAGS BIGINT NOT NULL, \n" +
        "    CONSTRAINT PK_VD PRIMARY KEY (RESOURCE_NAME, VLM_NR), \n" +
        "    CONSTRAINT FK_VD_RSC_DFN FOREIGN KEY (RESOURCE_NAME) REFERENCES RESOURCE_DEFINITIONS(RESOURCE_NAME) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_VD_UUID UNIQUE (UUID), \n" +
        "    CONSTRAINT UNQ_VD_MINOR UNIQUE (VLM_MINOR_NR) \n" +
        ")";
    public static final String CREATE_TABLE_VOLUMES =
        "CREATE TABLE VOLUMES \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME VARCHAR(255) NOT NULL, \n" +
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" +
        "    VLM_NR INT NOT NULL, \n" +
        "    STOR_POOL_NAME VARCHAR(32) NOT NULL, \n" +
        "    BLOCK_DEVICE_PATH VARCHAR(255), -- null == diskless \n" +
        "    META_DISK_PATH VARCHAR(255),  -- null == internal \n" +
        "    VLM_FLAGS BIGINT NOT NULL, \n" +
        "    CONSTRAINT PK_V PRIMARY KEY (NODE_NAME, RESOURCE_NAME, VLM_NR), \n" +
        "    CONSTRAINT FK_V_RSCS FOREIGN KEY (NODE_NAME, RESOURCE_NAME) REFERENCES RESOURCES(NODE_NAME, RESOURCE_NAME) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_V_VLM_DFNS FOREIGN KEY (RESOURCE_NAME, VLM_NR) REFERENCES VOLUME_DEFINITIONS(RESOURCE_NAME, VLM_NR) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_V_STOR_POOL_DFNS FOREIGN KEY (STOR_POOL_NAME) REFERENCES STOR_POOL_DEFINITIONS(POOL_NAME) \n" +
        "        ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_V_UUID UNIQUE (UUID) \n" +
        ")";
    public static final String CREATE_TABLE_NODE_CONNECTIONS =
        "CREATE TABLE NODE_CONNECTIONS \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME_SRC VARCHAR(255) NOT NULL, \n" +
        "    NODE_NAME_DST VARCHAR(255) NOT NULL, \n" +
        "    CONSTRAINT PK_NC PRIMARY KEY (NODE_NAME_SRC, NODE_NAME_DST), \n" +
        "    CONSTRAINT FK_NC_NODES_SRC FOREIGN KEY (NODE_NAME_SRC) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_NC_NODES_DST FOREIGN KEY (NODE_NAME_DST) REFERENCES NODES(NODE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_NC_UUID UNIQUE (UUID) \n" +
        ")";
    public static final String CREATE_TABLE_RESOURCE_CONNECTIONS =
        "CREATE TABLE RESOURCE_CONNECTIONS \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME_SRC VARCHAR(255) NOT NULL, \n" +
        "    NODE_NAME_DST VARCHAR(255) NOT NULL, \n" +
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" +
        "    CONSTRAINT PK_RC PRIMARY KEY (NODE_NAME_SRC, NODE_NAME_DST, RESOURCE_NAME), \n" +
        "    CONSTRAINT FK_RC_RSCS_SRC  FOREIGN KEY (NODE_NAME_SRC, RESOURCE_NAME) REFERENCES \n" +
        "        RESOURCES(NODE_NAME, RESOURCE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_RC_RSCS_DST FOREIGN KEY (NODE_NAME_DST, RESOURCE_NAME) REFERENCES \n" +
        "        RESOURCES(NODE_NAME, RESOURCE_NAME) ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_RC_UUID UNIQUE (UUID) \n" +
        ")";
    public static final String CREATE_TABLE_VOLUME_CONNECTIONS =
        "CREATE TABLE VOLUME_CONNECTIONS \n" +
        "( \n" +
        "    UUID CHAR(36) NOT NULL, \n" +
        "    NODE_NAME_SRC VARCHAR(255) NOT NULL, \n" +
        "    NODE_NAME_DST VARCHAR(255) NOT NULL, \n" +
        "    RESOURCE_NAME VARCHAR(48) NOT NULL, \n" +
        "    VLM_NR INT NOT NULL, \n" +
        "    CONSTRAINT PK_VC PRIMARY KEY (NODE_NAME_SRC, NODE_NAME_DST, RESOURCE_NAME, VLM_NR), \n" +
        "    CONSTRAINT FK_VC_VLMS_SRC FOREIGN KEY (NODE_NAME_SRC, RESOURCE_NAME, VLM_NR) REFERENCES \n" +
        "        VOLUMES(NODE_NAME, RESOURCE_NAME, VLM_NR) ON DELETE CASCADE, \n" +
        "    CONSTRAINT FK_VC_VLMS_DST FOREIGN KEY (NODE_NAME_DST, RESOURCE_NAME, VLM_NR) REFERENCES \n" +
        "        VOLUMES(NODE_NAME, RESOURCE_NAME, VLM_NR) ON DELETE CASCADE, \n" +
        "    CONSTRAINT UNQ_VC_UUID UNIQUE (UUID) \n" +
        ")";
    public static final String CREATE_TABLE_PROPS_CONTAINERS =
        "CREATE TABLE PROPS_CONTAINERS \n" +
        "( \n" +
        "    PROPS_INSTANCE VARCHAR(512) NOT NULL, \n" +
        "    PROP_KEY VARCHAR(512) NOT NULL, \n" +
        "    PROP_VALUE VARCHAR(4096) NOT NULL, \n" +
        "    CONSTRAINT PK_PC PRIMARY KEY (PROPS_INSTANCE, PROP_KEY), \n" +
        "    CONSTRAINT CHK_PC_PRP_INST_NAME CHECK(UPPER(PROPS_INSTANCE) = PROPS_INSTANCE AND LENGTH(PROPS_INSTANCE) >= 2) \n" +
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
    public static final String DROP_VIEW_SEC_TYPE_RULES_LOAD  = "DROP VIEW " + VIEW_SEC_TYPE_RULES_LOAD;
    public static final String DROP_VIEW_SEC_TYPES_LOAD       = "DROP VIEW " + VIEW_SEC_TYPES_LOAD;
    public static final String DROP_VIEW_SEC_ROLES_LOAD       = "DROP VIEW " + VIEW_SEC_ROLES_LOAD;
    public static final String DROP_VIEW_SEC_IDENTITIES_LOAD  = "DROP VIEW " + VIEW_SEC_IDENTITIES_LOAD;
    public static final String DROP_TBL_PROPS_CONTAINERS      = "DROP TABLE " + TBL_PROPS_CONTAINERS;
    public static final String DROP_TBL_VOLUME_CONNECTIONS    = "DROP TABLE " + TBL_VOLUME_CONNECTIONS;
    public static final String DROP_TBL_RESOURCE_CONNECTIONS  = "DROP TABLE " + TBL_RESOURCE_CONNECTIONS;
    public static final String DROP_TBL_NODE_CONNECTIONS      = "DROP TABLE " + TBL_NODE_CONNECTIONS;
    public static final String DROP_TBL_VOLUMES               = "DROP TABLE " + TBL_VOLUMES;
    public static final String DROP_TBL_VOLUME_DEFINITIONS    = "DROP TABLE " + TBL_VOLUME_DEFINITIONS;
    public static final String DROP_TBL_NODE_STOR_POOL        = "DROP TABLE " + TBL_NODE_STOR_POOL;
    public static final String DROP_TBL_STOR_POOL_DEFINITIONS = "DROP TABLE " + TBL_STOR_POOL_DEFINITIONS;
    public static final String DROP_TBL_RESOURCES             = "DROP TABLE " + TBL_RESOURCES;
    public static final String DROP_TBL_RESOURCE_DEFINITIONS  = "DROP TABLE " + TBL_RESOURCE_DEFINITIONS;
    public static final String DROP_TBL_SATELLITE_CONNECTIONS = "DROP TABLE " + TBL_SATELLITE_CONNECTIONS;
    public static final String DROP_TBL_NODE_NET_INTERFACES   = "DROP TABLE " + TBL_NODE_NET_INTERFACES;
    public static final String DROP_TBL_NODES                 = "DROP TABLE " + TBL_NODES;
    public static final String DROP_TBL_SEC_ACL_MAP           = "DROP TABLE " + TBL_SEC_ACL_MAP;
    public static final String DROP_TBL_SEC_OBJECT_PROTECTION = "DROP TABLE " + TBL_SEC_OBJECT_PROTECTION;
    public static final String DROP_TBL_SEC_DFLT_ROLES        = "DROP TABLE " + TBL_SEC_DFLT_ROLES;
    public static final String DROP_TBL_SEC_TYPE_RULES        = "DROP TABLE " + TBL_SEC_TYPE_RULES;
    public static final String DROP_TBL_SEC_ACCESS_TYPES      = "DROP TABLE " + TBL_SEC_ACCESS_TYPES;
    public static final String DROP_TBL_SEC_ID_ROLE_MAP       = "DROP TABLE " + TBL_SEC_ID_ROLE_MAP;
    public static final String DROP_TBL_SEC_ROLES             = "DROP TABLE " + TBL_SEC_ROLES;
    public static final String DROP_TBL_SEC_TYPES             = "DROP TABLE " + TBL_SEC_TYPES;
    public static final String DROP_TBL_SEC_IDENTITIES        = "DROP TABLE " + TBL_SEC_IDENTITIES;
    public static final String DROP_TBL_SEC_CONFIGURATION     = "DROP TABLE " + TBL_SEC_CONFIGURATION;

    // truncate statements
    public static final String TRUNCATE_PROPS_CONTAINERS      = "DELETE FROM " + TBL_PROPS_CONTAINERS;
    public static final String TRUNCATE_VOLUME_CONNECTIONS    = "DELETE FROM " + TBL_VOLUME_CONNECTIONS;
    public static final String TRUNCATE_RESOURCE_CONNECTIONS  = "DELETE FROM " + TBL_RESOURCE_CONNECTIONS;
    public static final String TRUNCATE_NODE_CONNECTIONS      = "DELETE FROM " + TBL_NODE_CONNECTIONS;
    public static final String TRUNCATE_VOLUMES               = "DELETE FROM " + TBL_VOLUMES;
    public static final String TRUNCATE_VOLUME_DEFINITIONS    = "DELETE FROM " + TBL_VOLUME_DEFINITIONS;
    public static final String TRUNCATE_NODE_STOR_POOL        = "DELETE FROM " + TBL_NODE_STOR_POOL;
    public static final String TRUNCATE_STOR_POOL_DEFINITIONS = "DELETE FROM " + TBL_STOR_POOL_DEFINITIONS;
    public static final String TRUNCATE_RESOURCES             = "DELETE FROM " + TBL_RESOURCES;
    public static final String TRUNCATE_RESOURCE_DEFINITIONS  = "DELETE FROM " + TBL_RESOURCE_DEFINITIONS;
    public static final String TRUNCATE_SATELLITE_CONNECTIONS = "DELETE FROM " + TBL_SATELLITE_CONNECTIONS;
    public static final String TRUNCATE_NODE_NET_INTERFACES   = "DELETE FROM " + TBL_NODE_NET_INTERFACES;
    public static final String TRUNCATE_NODES                 = "DELETE FROM " + TBL_NODES;
    public static final String TRUNCATE_SEC_ACL_MAP           = "DELETE FROM " + TBL_SEC_ACL_MAP;
    public static final String TRUNCATE_SEC_OBJECT_PROTECTION = "DELETE FROM " + TBL_SEC_OBJECT_PROTECTION;
    public static final String TRUNCATE_SEC_DFLT_ROLES        = "DELETE FROM " + TBL_SEC_DFLT_ROLES;
    public static final String TRUNCATE_SEC_TYPE_RULES        = "DELETE FROM " + TBL_SEC_TYPE_RULES;
    public static final String TRUNCATE_SEC_ACCESS_TYPES      = "DELETE FROM " + TBL_SEC_ACCESS_TYPES;
    public static final String TRUNCATE_SEC_ID_ROLE_MAP       = "DELETE FROM " + TBL_SEC_ID_ROLE_MAP;
    public static final String TRUNCATE_SEC_ROLES             = "DELETE FROM " + TBL_SEC_ROLES;
    public static final String TRUNCATE_SEC_TYPES             = "DELETE FROM " + TBL_SEC_TYPES;
    public static final String TRUNCATE_SEC_IDENTITIES        = "DELETE FROM " + TBL_SEC_IDENTITIES;
    public static final String TRUNCATE_SEC_CONFIGURATION     = "DELETE FROM " + TBL_SEC_CONFIGURATION;

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
        CREATE_TABLE_NODES,
        CREATE_TABLE_NODE_NET_INTERFACES,
        CREATE_TABLE_SATELLITE_CONNECTIONS,
        CREATE_TABLE_RESOURCE_DEFINITIONS,
        CREATE_TABLE_RESOURCES,
        CREATE_TABLE_STOR_POOL_DEFINITIONS,
        CREATE_TABLE_NODE_STOR_POOL,
        CREATE_TABLE_VOLUME_DEFINITIONS,
        CREATE_TABLE_VOLUMES,
        CREATE_TABLE_NODE_CONNECTIONS,
        CREATE_TABLE_RESOURCE_CONNECTIONS,
        CREATE_TABLE_VOLUME_CONNECTIONS,
        CREATE_TABLE_PROPS_CONTAINERS,
        CREATE_VIEW_SEC_IDENTITIES_LOAD,
        CREATE_VIEW_SEC_ROLES_LOAD,
        CREATE_VIEW_SEC_TYPES_LOAD,
        CREATE_VIEW_SEC_TYPE_RULES_LOAD,
    };

    // insert statements (default values)
    public static final String[] INSERT_DEFAULT_VALUES =
    {
        "INSERT INTO SEC_CONFIGURATION (ENTRY_KEY, ENTRY_DSP_KEY, ENTRY_VALUE) \n" +
        "    VALUES ('SECURITYLEVEL', 'SecurityLevel', 'MAC')",
        "INSERT INTO SEC_CONFIGURATION (ENTRY_KEY, ENTRY_DSP_KEY, ENTRY_VALUE) \n" +
        "    VALUES ('AUTHREQUIRED', 'AuthRequired', 'false')",
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" +
        "    VALUES ('CONTROL', 15)",
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" +
        "    VALUES ('CHANGE', 7)",
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" +
        "    VALUES ('USE', 3)",
        "INSERT INTO SEC_ACCESS_TYPES (ACCESS_TYPE_NAME, ACCESS_TYPE_VALUE) \n" +
        "    VALUES ('VIEW', 1)",
        "INSERT INTO SEC_IDENTITIES (IDENTITY_NAME, IDENTITY_DSP_NAME, ID_ENABLED, ID_LOCKED) \n" +
        "    VALUES ('SYSTEM', 'SYSTEM', TRUE, TRUE)",
        "INSERT INTO SEC_IDENTITIES (IDENTITY_NAME, IDENTITY_DSP_NAME, ID_ENABLED, ID_LOCKED) \n" +
        "    VALUES ('PUBLIC', 'PUBLIC', TRUE, TRUE)",
        "INSERT INTO SEC_TYPES (TYPE_NAME, TYPE_DSP_NAME, TYPE_ENABLED) \n" +
        "    VALUES ('SYSTEM', 'SYSTEM', TRUE)",
        "INSERT INTO SEC_TYPES (TYPE_NAME, TYPE_DSP_NAME, TYPE_ENABLED) \n" +
        "    VALUES ('PUBLIC', 'PUBLIC', TRUE)",
        "INSERT INTO SEC_TYPES (TYPE_NAME, TYPE_DSP_NAME, TYPE_ENABLED) \n" +
        "    VALUES ('SHARED', 'SHARED', TRUE)",
        "INSERT INTO SEC_TYPES (TYPE_NAME, TYPE_DSP_NAME, TYPE_ENABLED) \n" +
        "    VALUES ('SYSADM', 'SysAdm', TRUE)",
        "INSERT INTO SEC_TYPES (TYPE_NAME, TYPE_DSP_NAME, TYPE_ENABLED) \n" +
        "    VALUES ('USER', 'User', TRUE)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSTEM', 'SYSTEM', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSTEM', 'PUBLIC', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSTEM', 'SHARED', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSTEM', 'SYSADM', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSTEM', 'USER', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('PUBLIC', 'SYSTEM', 3)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('PUBLIC', 'PUBLIC', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('PUBLIC', 'SHARED', 7)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('PUBLIC', 'SYSADM', 3)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('PUBLIC', 'USER', 3)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSADM', 'SYSTEM', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSADM', 'PUBLIC', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSADM', 'SHARED', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSADM', 'SYSADM', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('SYSADM', 'USER', 15)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('USER', 'SYSTEM', 3)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('USER', 'PUBLIC', 7)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('USER', 'SHARED', 7)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('USER', 'SYSADM', 3)",
        "INSERT INTO SEC_TYPE_RULES (DOMAIN_NAME, TYPE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('USER', 'USER', 15)",
        "INSERT INTO SEC_ROLES (ROLE_NAME, ROLE_DSP_NAME, DOMAIN_NAME, ROLE_ENABLED, ROLE_PRIVILEGES) \n" +
        "    VALUES ('SYSTEM', 'SYSTEM', 'SYSTEM', TRUE, -1)",
        "INSERT INTO SEC_ROLES (ROLE_NAME, ROLE_DSP_NAME, DOMAIN_NAME, ROLE_ENABLED, ROLE_PRIVILEGES) \n" +
        "    VALUES ('PUBLIC', 'PUBLIC', 'PUBLIC', TRUE, 0)",
        "INSERT INTO SEC_ROLES (ROLE_NAME, ROLE_DSP_NAME, DOMAIN_NAME, ROLE_ENABLED, ROLE_PRIVILEGES) \n" +
        "    VALUES ('SYSADM', 'SysAdm', 'SYSADM', TRUE, -1)",
        "INSERT INTO SEC_ROLES (ROLE_NAME, ROLE_DSP_NAME, DOMAIN_NAME, ROLE_ENABLED, ROLE_PRIVILEGES) \n" +
        "    VALUES ('USER', 'User', 'USER', TRUE, 0)",
        "INSERT INTO SEC_ID_ROLE_MAP (IDENTITY_NAME, ROLE_NAME) \n" +
        "    VALUES ('SYSTEM', 'SYSTEM')",
        "INSERT INTO SEC_ID_ROLE_MAP (IDENTITY_NAME, ROLE_NAME) \n" +
        "    VALUES ('PUBLIC', 'PUBLIC')",
        "INSERT INTO SEC_DFLT_ROLES (IDENTITY_NAME, ROLE_NAME) \n" +
        "    VALUES ('SYSTEM', 'SYSTEM')",
        "INSERT INTO SEC_DFLT_ROLES (IDENTITY_NAME, ROLE_NAME) \n" +
        "    VALUES ('PUBLIC', 'PUBLIC')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/PlainConnector/type', 'plain')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/PlainConnector/bindaddress', '::0')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/PlainConnector/port', '3376')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/type', 'ssl')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/bindaddress', '::0')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/port', '3377')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/keyPasswd', 'linstor')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/keyStorePasswd', 'linstor')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/trustStorePasswd', 'linstor')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/trustStore', 'ssl/certificates.jks')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/sslProtocol', 'TLSv1')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/SslConnector/keyStore', 'ssl/keystore.jks')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/type', 'ssl')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/bindaddress', '::0')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/port', '3373')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/keyPasswd', 'linstor')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/keyStorePasswd', 'linstor')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/trustStorePasswd', 'linstor')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/trustStore', 'ssl/certificates.jks')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/sslProtocol', 'TLSv1')",
        "INSERT INTO PROPS_CONTAINERS VALUES ('CTRLCFG', 'netcom/DebugSslConnector/keyStore', 'ssl/keystore.jks')",
        "INSERT INTO SEC_OBJECT_PROTECTION (OBJECT_PATH, CREATOR_IDENTITY_NAME, OWNER_ROLE_NAME, SECURITY_TYPE_NAME) \n" +
        "    VALUES ('/sys/controller/nodesMap', 'SYSTEM', 'SYSADM', 'SHARED')",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/nodesMap', 'SYSTEM', 15)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/nodesMap', 'USER', 7)",
        "INSERT INTO SEC_OBJECT_PROTECTION (OBJECT_PATH, CREATOR_IDENTITY_NAME, OWNER_ROLE_NAME, SECURITY_TYPE_NAME) \n" +
        "    VALUES ('/sys/controller/rscDfnMap', 'SYSTEM', 'SYSADM', 'SHARED')",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/rscDfnMap', 'SYSTEM', 15)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/rscDfnMap', 'USER', 7)",
        "INSERT INTO SEC_OBJECT_PROTECTION (OBJECT_PATH, CREATOR_IDENTITY_NAME, OWNER_ROLE_NAME, SECURITY_TYPE_NAME) \n" +
        "    VALUES ('/sys/controller/storPoolMap', 'SYSTEM', 'SYSADM', 'SHARED')",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/storPoolMap', 'SYSTEM', 15)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/storPoolMap', 'USER', 7)",
        "INSERT INTO SEC_OBJECT_PROTECTION (OBJECT_PATH, CREATOR_IDENTITY_NAME, OWNER_ROLE_NAME, SECURITY_TYPE_NAME) \n" +
        "    VALUES ('/sys/controller/conf', 'SYSTEM', 'SYSADM', 'SYSTEM')",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/conf', 'SYSTEM', 15)",
        "INSERT INTO SEC_OBJECT_PROTECTION (OBJECT_PATH, CREATOR_IDENTITY_NAME, OWNER_ROLE_NAME, SECURITY_TYPE_NAME) \n" +
        "    VALUES ('/sys/controller/shutdown', 'SYSTEM', 'SYSADM', 'SYSTEM')",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/shutdown', 'SYSTEM', 15)",
        "INSERT INTO STOR_POOL_DEFINITIONS VALUES ('f51611c6-528f-4793-a87a-866d09e6733a', 'DFLTSTORPOOL', 'DfltStorPool')",
        "INSERT INTO SEC_OBJECT_PROTECTION (OBJECT_PATH, CREATOR_IDENTITY_NAME, OWNER_ROLE_NAME, SECURITY_TYPE_NAME) \n" +
        "    VALUES ('/storpooldefinitions/DFLTSTORPOOL', 'SYSTEM', 'SYSADM', 'SHARED')",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/storpooldefinitions/DFLTSTORPOOL', 'PUBLIC', 7)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/storpooldefinitions/DFLTSTORPOOL', 'USER', 7)",
        "INSERT INTO STOR_POOL_DEFINITIONS VALUES ('622807eb-c8c4-44f0-b03d-a08173c8fa1b', 'DFLTDISKLESSSTORPOOL', 'DfltDisklessStorPool')",
        "INSERT INTO SEC_OBJECT_PROTECTION (OBJECT_PATH, CREATOR_IDENTITY_NAME, OWNER_ROLE_NAME, SECURITY_TYPE_NAME) \n" +
        "    VALUES ('/storpooldefinitions/DFLTDISKLESSSTORPOOL', 'SYSTEM', 'SYSADM', 'SHARED')",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/storpooldefinitions/DFLTDISKLESSSTORPOOL', 'PUBLIC', 7)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/storpooldefinitions/DFLTDISKLESSSTORPOOL', 'USER', 7)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/nodesMap', 'PUBLIC', 7)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/rscDfnMap', 'PUBLIC', 7)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/storPoolMap', 'PUBLIC', 7)",
        "INSERT INTO SEC_ACL_MAP (OBJECT_PATH, ROLE_NAME, ACCESS_TYPE) \n" +
        "    VALUES ('/sys/controller/conf', 'PUBLIC', 1)",
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
    public static final String INSERT_NODES =
        " INSERT INTO " + TBL_NODES +
        " VALUES (?, ?, ?, ?, ?)";
    public static final String INSERT_NODE_NET_INTERFACES =
        " INSERT INTO " + TBL_NODE_NET_INTERFACES +
        " VALUES (?, ?, ?, ?, ?)";
    public static final String INSERT_SATELLITE_CONNECTIONS =
        " INSERT INTO " + TBL_SATELLITE_CONNECTIONS +
        " VALUES (?, ?, ?, ?, ?)";
    public static final String INSERT_RESOURCE_DEFINITIONS =
        " INSERT INTO " + TBL_RESOURCE_DEFINITIONS +
        " VALUES (?, ?, ?, ?, ?, ?, ?)";
    public static final String INSERT_RESOURCES =
        " INSERT INTO " + TBL_RESOURCES +
        " VALUES (?, ?, ?, ?, ?)";
    public static final String INSERT_STOR_POOL_DEFINITIONS =
        " INSERT INTO " + TBL_STOR_POOL_DEFINITIONS +
        " VALUES (?, ?, ?)";
    public static final String INSERT_NODE_STOR_POOL =
        " INSERT INTO " + TBL_NODE_STOR_POOL +
        " VALUES (?, ?, ?, ?)";
    public static final String INSERT_VOLUME_DEFINITIONS =
        " INSERT INTO " + TBL_VOLUME_DEFINITIONS +
        " VALUES (?, ?, ?, ?, ?, ?)";
    public static final String INSERT_VOLUMES =
        " INSERT INTO " + TBL_VOLUMES +
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    public static final String INSERT_NODE_CONNECTIONS =
        " INSERT INTO " + TBL_NODE_CONNECTIONS +
        " VALUES (?, ?, ?)";
    public static final String INSERT_RESOURCE_CONNECTIONS =
        " INSERT INTO " + TBL_RESOURCE_CONNECTIONS +
        " VALUES (?, ?, ?, ?)";
    public static final String INSERT_VOLUME_CONNECTIONS =
        " INSERT INTO " + TBL_VOLUME_CONNECTIONS +
        " VALUES (?, ?, ?, ?, ?)";
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
        DROP_TBL_VOLUME_CONNECTIONS,
        DROP_TBL_RESOURCE_CONNECTIONS,
        DROP_TBL_NODE_CONNECTIONS,
        DROP_TBL_VOLUMES,
        DROP_TBL_VOLUME_DEFINITIONS,
        DROP_TBL_NODE_STOR_POOL,
        DROP_TBL_STOR_POOL_DEFINITIONS,
        DROP_TBL_RESOURCES,
        DROP_TBL_RESOURCE_DEFINITIONS,
        DROP_TBL_SATELLITE_CONNECTIONS,
        DROP_TBL_NODE_NET_INTERFACES,
        DROP_TBL_NODES,
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
        TRUNCATE_VOLUME_CONNECTIONS,
        TRUNCATE_RESOURCE_CONNECTIONS,
        TRUNCATE_NODE_CONNECTIONS,
        TRUNCATE_VOLUMES,
        TRUNCATE_VOLUME_DEFINITIONS,
        TRUNCATE_NODE_STOR_POOL,
        TRUNCATE_STOR_POOL_DEFINITIONS,
        TRUNCATE_RESOURCES,
        TRUNCATE_RESOURCE_DEFINITIONS,
        TRUNCATE_SATELLITE_CONNECTIONS,
        TRUNCATE_NODE_NET_INTERFACES,
        TRUNCATE_NODES,
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
