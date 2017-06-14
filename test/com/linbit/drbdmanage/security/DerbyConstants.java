package com.linbit.drbdmanage.security;

public interface DerbyConstants
{
    /*
     * Database names
     */

    // View names
    public static final String VIEW_SEC_IDENTITIES_LOAD = "SEC_IDENTITIES_LOAD";
    public static final String VIEW_SEC_ROLES_LOAD = "SEC_ROLES_LOAD";
    public static final String VIEW_SEC_TYPES_LOAD = "SEC_TYPES_LOAD";
    public static final String VIEW_SEC_TYPE_RULES_LOAD = "SEC_TYPE_RULES_LOAD";

    // Table names
    public static final String TBL_SEC_CONFIGURATION = "SEC_CONFIGURATION";
    public static final String TBL_SEC_IDENTITIES = "SEC_IDENTITIES";
    public static final String TBL_SEC_TYPES = "SEC_TYPES";
    public static final String TBL_SEC_ROLES = "SEC_ROLES";
    public static final String TBL_SEC_ID_ROLE_MAP = "SEC_ID_ROLE_MAP";
    public static final String TBL_SEC_ACCESS_TYPES = "SEC_ACCESS_TYPES";
    public static final String TBL_SEC_TYPE_RULES = "SEC_TYPE_RULES";
    public static final String TBL_SEC_DFLT_ROLES = "SEC_DFLT_ROLES";
    public static final String TBL_SEC_OBJECT_PROTECTION = "SEC_OBJECT_PROTECTION";
    public static final String TBL_SEC_ACL_MAP = "SEC_ACL_MAP";

    // SEC_CONFIGURATION column names
    public static final String SEC_CONFIG_ENTRY_KEY = "ENTRY_KEY";
    public static final String SEC_CONFIG_ENTRY_DSP_KEY = "ENTRY_DSP_KEY";
    public static final String SEC_CONFIG_ENTRY_VALUE = "ENTRY_VALUE";

    // SEC_IDENTITIES column names
    public static final String ID_NAME = "IDENTITY_NAME";
    public static final String ID_DSP_NAME = "IDENTITY_DSP_NAME";
    public static final String ID_PASS_SALT = "PASS_SALT";
    public static final String ID_PASS_HASH = "PASS_HASH";
    public static final String ID_ENABLED = "ID_ENABLED";
    public static final String ID_LOCKED = "ID_LOCKED";

    // SEC_TYPES column names
    public static final String TYPE_NAME = "TYPE_NAME";
    public static final String TYPE_DSP_NAME = "TYPE_DSP_NAME";
    public static final String TYPE_ENABLED = "TYPE_ENABLED";

    // SEC_ROLES column names
    public static final String ROLE_NAME = "ROLE_NAME";
    public static final String ROLE_DSP_NAME = "ROLE_DSP_NAME";
    public static final String DOMAIN_NAME = "DOMAIN_NAME";
    public static final String ROLE_ENABLED = "ROLE_ENABLED";
    public static final String ROLE_PRIVILEGES = "ROLE_PRIVILEGES";

    // SEC_ACCESS_TYPES column names
    public static final String ACCESS_TYPE_NAME = "ACCESS_TYPE_NAME";
    public static final String ACCESS_TYPE_VALUE = "ACCESS_TYPE_VALUE";
    public static final String ACCESS_TYPE = "ACCESS_TYPE";

    // SEC_OBJECT_PROTECTION column names
    public static final String OBJECT_PATH = "OBJECT_PATH";
    public static final String CREATOR_IDENTITY_NAME = "CREATOR_IDENTITY_NAME";
    public static final String OWNER_ROLE_NAME = "OWNER_ROLE_NAME";
    public static final String SECURITY_TYPE_NAME = "SECURITY_TYPE_NAME";


    /* ***************************************
     *                                       *
     *          CREATE statements            *
     *                                       *
     *************************************** */

    public static final String CREATE_TABLE_SEC_CONFIG =
        "CREATE TABLE " + TBL_SEC_CONFIGURATION + " \n" +
        "( \n" +
        "   " + SEC_CONFIG_ENTRY_KEY + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
        "       CONSTRAINT SEC_CONF_CHKKEY CHECK (UPPER(" + SEC_CONFIG_ENTRY_KEY + ") = " + SEC_CONFIG_ENTRY_KEY + " AND LENGTH(" + SEC_CONFIG_ENTRY_KEY + ") >= 3), \n" +
        "   " + SEC_CONFIG_ENTRY_DSP_KEY + " VARCHAR(24) NOT NULL, \n" +
        "   " + SEC_CONFIG_ENTRY_VALUE + " VARCHAR(24) NOT NULL, \n" +
        "       CONSTRAINT SEC_CONF_CHKDSPKEY CHECK (UPPER(" + SEC_CONFIG_ENTRY_DSP_KEY + ") = " + SEC_CONFIG_ENTRY_KEY + ") \n" +
        ")";
    public static final String CREATE_TABLE_SEC_IDENTITIES =
        "CREATE TABLE " + TBL_SEC_IDENTITIES + " \n" +
        "( \n" +
        "   " + ID_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
        "       CONSTRAINT SEC_ID_CHKNAME CHECK (UPPER(" + ID_NAME + ") = " + ID_NAME + " AND LENGTH(" + ID_NAME + ") >= 3), \n" +
        "   " + ID_DSP_NAME + " VARCHAR(24) NOT NULL, \n" +
        "   " + ID_PASS_SALT + " CHAR(16) FOR BIT DATA, \n" +
        "   " + ID_PASS_HASH + " CHAR(64) FOR BIT DATA, \n" +
        "   " + ID_ENABLED + " BOOLEAN NOT NULL DEFAULT TRUE, \n" +
        "   " + ID_LOCKED + " BOOLEAN NOT NULL DEFAULT TRUE, \n" +
        "   CONSTRAINT SEC_ID_CHKDSPNAME CHECK (UPPER(" + ID_DSP_NAME + ") = " + ID_NAME + ") \n" +
        ")";
    public static final String CREATE_TABLE_SEC_TYPES =
        "CREATE TABLE " + TBL_SEC_TYPES + " \n" +
        "( \n" +
        "    " + TYPE_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
        "        CONSTRAINT SEC_TYPES_CHKNAME CHECK (UPPER(" + TYPE_NAME + ") = " + TYPE_NAME + " AND LENGTH(" + TYPE_NAME + ") >= 3), \n" +
        "    " + TYPE_DSP_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + TYPE_ENABLED + " BOOLEAN NOT NULL DEFAULT TRUE, \n" +
        "    CONSTRAINT SEC_TYPES_CHKDSPNAME CHECK (UPPER(" + TYPE_DSP_NAME + ") = " + TYPE_NAME + ") \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ROLES =
        "CREATE TABLE " + TBL_SEC_ROLES + " \n" +
        "( \n" +
        "    " + ROLE_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
        "        CONSTRAINT SEC_ROLES_CHKNAME CHECK (UPPER(" + ROLE_NAME + ") = " + ROLE_NAME + " AND LENGTH(" + ROLE_NAME + ") >= 3), \n" +
        "    " + ROLE_DSP_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + DOMAIN_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + ROLE_ENABLED + " BOOLEAN NOT NULL DEFAULT TRUE, \n" +
        "    " + ROLE_PRIVILEGES + " BIGINT NOT NULL DEFAULT 0, \n" +
        "    FOREIGN KEY (" + DOMAIN_NAME + ") REFERENCES " + TBL_SEC_TYPES + "(" + TYPE_NAME + "), \n" +
        "    CONSTRAINT SEC_ROLES_CHKDSPNAME CHECK (UPPER(" + ROLE_DSP_NAME + ") = " + ROLE_NAME + ") \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ID_ROLE_MAP =
        "CREATE TABLE " + TBL_SEC_ID_ROLE_MAP + " \n" +
        "( \n" +
        "    " + ID_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    PRIMARY KEY (" + ID_NAME + ", " + ROLE_NAME + "), \n" +
        "    FOREIGN KEY (" + ID_NAME + ") REFERENCES " + TBL_SEC_IDENTITIES + "(" + ID_NAME + ") ON DELETE CASCADE, \n" +
        "    FOREIGN KEY (" + ROLE_NAME + ") REFERENCES " + TBL_SEC_ROLES + "(" + ROLE_NAME + ") ON DELETE CASCADE \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ACCESS_TYPES =
        "CREATE TABLE " + TBL_SEC_ACCESS_TYPES + " \n" +
        "( \n" +
        "    " + ACCESS_TYPE_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
        "        CONSTRAINT SEC_ACCESS_TYPES_CHKNAME CHECK (UPPER(" + ACCESS_TYPE_NAME + ") = " + ACCESS_TYPE_NAME + "), \n" +
        "    " + ACCESS_TYPE_VALUE + " SMALLINT NOT NULL UNIQUE \n" +
        ")";
    public static final String CREATE_TABLE_SEC_TYPE_RULES =
        "CREATE TABLE " + TBL_SEC_TYPE_RULES + " \n" +
        "( \n" +
        "    " + DOMAIN_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + TYPE_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + ACCESS_TYPE + " SMALLINT NOT NULL, \n" +
        "    PRIMARY KEY (" + DOMAIN_NAME + ", " + TYPE_NAME + "), \n" +
        "    FOREIGN KEY (" + DOMAIN_NAME + ") REFERENCES " + TBL_SEC_TYPES + "(" + TYPE_NAME + ") ON DELETE CASCADE, \n" +
        "    FOREIGN KEY (" + TYPE_NAME + ") REFERENCES " + TBL_SEC_TYPES + "(" + TYPE_NAME + ") ON DELETE CASCADE, \n" +
        "    FOREIGN KEY (" + ACCESS_TYPE + ") REFERENCES " + TBL_SEC_ACCESS_TYPES + "(" + ACCESS_TYPE_VALUE + ") ON DELETE RESTRICT \n" +
        ")";
    public static final String CREATE_TABLE_SEC_DFLT_ROLES =
        "CREATE TABLE " + TBL_SEC_DFLT_ROLES + " \n" +
        "( \n" +
        "    " + ID_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY, \n" +
        "    " + ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    FOREIGN KEY (" + ID_NAME + ", " + ROLE_NAME + ") REFERENCES " + TBL_SEC_ID_ROLE_MAP + "(" + ID_NAME + ", " + ROLE_NAME + ") \n" +
        "        ON DELETE CASCADE \n" +
        ")";
    public static final String CREATE_TABLE_SEC_OBJECT_PROTECTION =
        "CREATE TABLE " + TBL_SEC_OBJECT_PROTECTION + " \n" +
        "( \n" +
        "    " + OBJECT_PATH + " VARCHAR(512) NOT NULL PRIMARY KEY, \n" +
        "    " + CREATOR_IDENTITY_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + OWNER_ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + SECURITY_TYPE_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    FOREIGN KEY (" + CREATOR_IDENTITY_NAME + ") REFERENCES " + TBL_SEC_IDENTITIES + "(" + ID_NAME + ") ON DELETE RESTRICT, \n" +
        "    FOREIGN KEY (" + OWNER_ROLE_NAME + ") REFERENCES " + TBL_SEC_ROLES + "(" + ROLE_NAME + ") ON DELETE RESTRICT, \n" +
        "    FOREIGN KEY (" + SECURITY_TYPE_NAME + ") REFERENCES " + TBL_SEC_TYPES + "(" + TYPE_NAME + ") ON DELETE RESTRICT \n" +
        ")";
    public static final String CREATE_TABLE_SEC_ACL_MAP =
        "CREATE TABLE " + TBL_SEC_ACL_MAP + " \n" +
        "( \n" +
        "    " + OBJECT_PATH + " VARCHAR(512) NOT NULL, \n" +
        "    " + ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
        "    " + ACCESS_TYPE + " SMALLINT NOT NULL, \n" +
        "    PRIMARY KEY (" + OBJECT_PATH + ", " + ROLE_NAME + "), \n" +
        "    FOREIGN KEY (" + OBJECT_PATH + ") REFERENCES " + TBL_SEC_OBJECT_PROTECTION + "(" + OBJECT_PATH + ") ON DELETE CASCADE, \n" +
        "    FOREIGN KEY (" + ROLE_NAME + ") REFERENCES " + TBL_SEC_ROLES + "(" + ROLE_NAME + ") ON DELETE RESTRICT, \n" +
        "    FOREIGN KEY (" + ACCESS_TYPE + ") REFERENCES " + TBL_SEC_ACCESS_TYPES + "(" + ACCESS_TYPE_VALUE + ") ON DELETE RESTRICT \n" +
        ")";
    public static final String CREATE_VIEW_SEC_ID_LOAD =
        "CREATE VIEW " + VIEW_SEC_IDENTITIES_LOAD + " AS " +
        "    SELECT " + ID_DSP_NAME + ", " + ID_ENABLED + " " +
        "    FROM " + TBL_SEC_IDENTITIES;
    public static final String CREATE_VIEW_SEC_ROLES_LOAD =
        "CREATE VIEW " + VIEW_SEC_ROLES_LOAD + " AS " +
        "    SELECT " + ROLE_DSP_NAME + ", " + ROLE_ENABLED +
        "    FROM " + TBL_SEC_ROLES;
    public static final String CREATE_VIEW_SEC_TYPES_LOAD =
        "CREATE VIEW " + VIEW_SEC_TYPES_LOAD + " AS " +
        "    SELECT " + TYPE_DSP_NAME + ", " + TYPE_ENABLED +
        "    FROM " + TBL_SEC_TYPES;
    public static final String CREATE_VIEW_SEC_TYPES_RULES_LOAD =
        "CREATE VIEW " + VIEW_SEC_TYPE_RULES_LOAD + " AS " +
        "    SELECT " + DOMAIN_NAME + ", " + TYPE_NAME + ", " + TBL_SEC_ACCESS_TYPES + "." + ACCESS_TYPE_NAME + " " + "AS " + ACCESS_TYPE +
        "    FROM " + TBL_SEC_TYPE_RULES +
        "    LEFT JOIN " + TBL_SEC_ACCESS_TYPES +
        "        ON " + TBL_SEC_TYPE_RULES + "." + ACCESS_TYPE + " = " + TBL_SEC_ACCESS_TYPES + "." + ACCESS_TYPE_VALUE +
        "    ORDER BY " + DOMAIN_NAME + ", " + TYPE_NAME + " ASC";

    /* ***************************************
     *                                       *
     *          INSERT statements            *
     *                                       *
     *************************************** */

    public static final String INSERT_SEC_ACCESS_TYPE_CONTROL = "INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
        " VALUES ('CONTROL', 15)";
    public static final String INSERT_SEC_ACCESS_TYPE_CHANGE = "INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
        " VALUES ('CHANGE', 7)";
    public static final String INSERT_SEC_ACCESS_TYPES_USE = "INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
        " VALUES ('USE', 3)";
    public static final String INSERT_SEC_ACCESS_TYPES_VIEW =
        " INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
        " VALUES ('VIEW', 1)";
    public static final String INSERT_SEC_ID_SYSTEM =
        " INSERT INTO " + TBL_SEC_IDENTITIES + " (" + ID_NAME + ", " + ID_DSP_NAME + ", " + ID_ENABLED + ", " + ID_LOCKED + ") " +
        " VALUES('SYSTEM', 'SYSTEM', TRUE, TRUE)";
    public static final String INSERT_SEC_ID_PUBLIC =
        " INSERT INTO " + TBL_SEC_IDENTITIES + " (" + ID_NAME + ", " + ID_DSP_NAME + ", " + ID_ENABLED + ", " + ID_LOCKED + ") " +
        " VALUES('PUBLIC', 'PUBLIC', TRUE, TRUE)";
    public static final String INSERT_SEC_TYPES_SYSTEM =
        " INSERT INTO " + TBL_SEC_TYPES + " (" + TYPE_NAME + ", " + TYPE_DSP_NAME + ", " + TYPE_ENABLED + ") " +
        " VALUES ('SYSTEM', 'SYSTEM', TRUE)";
    public static final String INSERT_SEC_TYPES_PUBLIC =
        " INSERT INTO " + TBL_SEC_TYPES + " (" + TYPE_NAME + ", " + TYPE_DSP_NAME + ", " + TYPE_ENABLED + ") " +
        " VALUES ('PUBLIC', 'PUBLIC', TRUE)";
    public static final String INSERT_SEC_ROLES_SYSTEM =
        " INSERT INTO " + TBL_SEC_ROLES + " (" + ROLE_NAME + ", " + ROLE_DSP_NAME + ", " + DOMAIN_NAME + ", " + ROLE_ENABLED + ", " + ROLE_PRIVILEGES + ") " +
        " VALUES('SYSTEM', 'SYSTEM', 'SYSTEM', TRUE, -9223372036854775808)";
    public static final String INSERT_SEC_ROLES_PUBLIC =
        " INSERT INTO " + TBL_SEC_ROLES + " (" + ROLE_NAME + ", " + ROLE_DSP_NAME + ", " + DOMAIN_NAME + ", " + ROLE_ENABLED + ", " + ROLE_PRIVILEGES + ") " +
        " VALUES('PUBLIC', 'PUBLIC', 'PUBLIC', TRUE, 0)";
    public static final String INSERT_SEC_ID_ROLE_MAP_SYSTEM =
        " INSERT INTO " + TBL_SEC_ID_ROLE_MAP + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
        " VALUES ('SYSTEM', 'SYSTEM')";
    public static final String INSERT_SEC_ID_ROLE_MAP_PUBLIC =
        " INSERT INTO " + TBL_SEC_ID_ROLE_MAP + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
        " VALUES ('PUBLIC', 'PUBLIC')";
    public static final String INSERT_SEC_DFLT_ROLES_SYSTEM =
        " INSERT INTO " + TBL_SEC_DFLT_ROLES + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
        " VALUES ('SYSTEM', 'SYSTEM')";
    public static final String INSERT_SEC_DFLT_ROLES_PUBLIC =
        " INSERT INTO " + TBL_SEC_DFLT_ROLES + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
        " VALUES ('PUBLIC', 'PUBLIC')";
    public static final String INSERT_SEC_CONF_MAC =
        " INSERT INTO " + TBL_SEC_CONFIGURATION + " (" + SEC_CONFIG_ENTRY_KEY + ", " + SEC_CONFIG_ENTRY_DSP_KEY + ", " + SEC_CONFIG_ENTRY_VALUE + ") " +
        " VALUES ('SECURITYLEVEL', 'SecurityLevel', 'MAC')";

    /* ***************************************
     *                                       *
     *           DROP statements             *
     *                                       *
     *************************************** */


    public static final String DROP_VIEW_SEC_TYPE_RULES_LOAD = "DROP VIEW " + VIEW_SEC_TYPE_RULES_LOAD;
    public static final String DROP_VIEW_SEC_TYPES_LOAD = "DROP VIEW " + VIEW_SEC_TYPES_LOAD;
    public static final String DROP_VIEW_SEC_ROLES_LOAD = "DROP VIEW " + VIEW_SEC_ROLES_LOAD;
    public static final String DROP_VIEW_SEC_IDENTITIES_LOAD = "DROP VIEW " + VIEW_SEC_IDENTITIES_LOAD;
    public static final String DROP_TBL_SEC_ACL_MAP = "DROP TABLE " + TBL_SEC_ACL_MAP;
    public static final String DROP_TBL_SEC_OBJECT_PROTECTION = "DROP TABLE " + TBL_SEC_OBJECT_PROTECTION;
    public static final String DORP_TBL_SEC_DFLT_ROLES = "DROP TABLE " + TBL_SEC_DFLT_ROLES;
    public static final String DROP_TBL_SEC_TYPE_RULES = "DROP TABLE " + TBL_SEC_TYPE_RULES;
    public static final String DROP_TBL_SEC_ACCESS_TYPES = "DROP TABLE " + TBL_SEC_ACCESS_TYPES;
    public static final String DROP_TBL_SEC_ID_ROLE_MAP = "DROP TABLE " + TBL_SEC_ID_ROLE_MAP;
    public static final String DROP_TBL_SEC_ROLES = "DROP TABLE " + TBL_SEC_ROLES;
    public static final String DROP_TBL_SEC_TYPES = "DROP TABLE " + TBL_SEC_TYPES;
    public static final String DROP_TBL_SEC_IDENTITIES = "DROP TABLE " + TBL_SEC_IDENTITIES;
    public static final String DROP_TBL_SEC_CONFIG = "DROP TABLE " + TBL_SEC_CONFIGURATION;

    /* ***************************************
     *                                       *
     *         TRUNCATE statements           *
     *                                       *
     *************************************** */

    public static final String TRUNCATE_SEC_ACL_MAP = "DELETE FROM " + TBL_SEC_ACL_MAP + " WHERE 1=1";
    public static final String TRUNCATE_SEC_OBJECT_PROTECTION = "DELETE FROM " + TBL_SEC_OBJECT_PROTECTION + " WHERE 1=1";
    public static final String TRUNCATE_SEC_DFLT_ROLES = "DELETE FROM " + TBL_SEC_DFLT_ROLES + " WHERE 1=1";
    public static final String TRUNCATE_SEC_TYPE_RULES = "DELETE FROM " + TBL_SEC_TYPE_RULES + " WHERE 1=1";
    public static final String TRUNCATE_SEC_ACCESS_TYPES = "DELETE FROM " + TBL_SEC_ACCESS_TYPES + " WHERE 1=1";
    public static final String TRUNCATE_SEC_ID_ROLE_MAP = "DELETE FROM " + TBL_SEC_ID_ROLE_MAP + " WHERE 1=1";
    public static final String TRUNCATE_SEC_ROLES = "DELETE FROM " + TBL_SEC_ROLES + " WHERE 1=1";
    public static final String TRUNCATE_SEC_TYPES = "DELETE FROM " + TBL_SEC_TYPES + " WHERE 1=1";
    public static final String TRUNCATE_SEC_IDENTITIES = "DELETE FROM " + TBL_SEC_IDENTITIES + " WHERE 1=1";
    public static final String TRUNCATE_SEC_CONFIG = "DELETE FROM " + TBL_SEC_CONFIGURATION + " WHERE 1=1";

    /* ***************************************
     *                                       *
     *         Convenience arrays            *
     *                                       *
     *************************************** */

    public static final String[] CREATE_SECURITY_TABLES = new String[]
    {
        CREATE_TABLE_SEC_CONFIG,
        CREATE_TABLE_SEC_IDENTITIES,
        CREATE_TABLE_SEC_TYPES,
        CREATE_TABLE_SEC_ROLES,
        CREATE_TABLE_SEC_ID_ROLE_MAP,
        CREATE_TABLE_SEC_ACCESS_TYPES,
        CREATE_TABLE_SEC_TYPE_RULES,
        CREATE_TABLE_SEC_DFLT_ROLES,
        CREATE_TABLE_SEC_OBJECT_PROTECTION,
        CREATE_TABLE_SEC_ACL_MAP,
        CREATE_VIEW_SEC_ID_LOAD,
        CREATE_VIEW_SEC_ROLES_LOAD,
        CREATE_VIEW_SEC_TYPES_LOAD,
        CREATE_VIEW_SEC_TYPES_RULES_LOAD
    };
    public static final String[] INSERT_SECURITY_DEFAULTS = new String[]
    {
        INSERT_SEC_ACCESS_TYPE_CONTROL,
        INSERT_SEC_ACCESS_TYPE_CHANGE,
        INSERT_SEC_ACCESS_TYPES_USE,
        INSERT_SEC_ACCESS_TYPES_VIEW,
        INSERT_SEC_ID_SYSTEM,
        INSERT_SEC_ID_PUBLIC,
        INSERT_SEC_TYPES_SYSTEM,
        INSERT_SEC_TYPES_PUBLIC,
        INSERT_SEC_ROLES_SYSTEM,
        INSERT_SEC_ROLES_PUBLIC,
        INSERT_SEC_ID_ROLE_MAP_SYSTEM,
        INSERT_SEC_ID_ROLE_MAP_PUBLIC,
        INSERT_SEC_DFLT_ROLES_SYSTEM,
        INSERT_SEC_DFLT_ROLES_PUBLIC,
        INSERT_SEC_CONF_MAC
    };
    public static final String[] TRUNCATE_SECURITY_TABLES = new String[]
    {
        TRUNCATE_SEC_ACL_MAP,
        TRUNCATE_SEC_OBJECT_PROTECTION,
        TRUNCATE_SEC_DFLT_ROLES,
        TRUNCATE_SEC_TYPE_RULES,
        TRUNCATE_SEC_ACCESS_TYPES,
        TRUNCATE_SEC_ID_ROLE_MAP,
        TRUNCATE_SEC_ROLES,
        TRUNCATE_SEC_TYPES,
        TRUNCATE_SEC_IDENTITIES,
        TRUNCATE_SEC_CONFIG
    };
    public static final String[] DROP_SECURITY_TABLES = new String[]
    {
        DROP_VIEW_SEC_TYPE_RULES_LOAD,
        DROP_VIEW_SEC_TYPES_LOAD,
        DROP_VIEW_SEC_ROLES_LOAD,
        DROP_VIEW_SEC_IDENTITIES_LOAD,
        DROP_TBL_SEC_ACL_MAP,
        DROP_TBL_SEC_OBJECT_PROTECTION,
        DORP_TBL_SEC_DFLT_ROLES,
        DROP_TBL_SEC_TYPE_RULES,
        DROP_TBL_SEC_ACCESS_TYPES,
        DROP_TBL_SEC_ID_ROLE_MAP,
        DROP_TBL_SEC_ROLES,
        DROP_TBL_SEC_TYPES,
        DROP_TBL_SEC_IDENTITIES,
        DROP_TBL_SEC_CONFIG
    };



}
