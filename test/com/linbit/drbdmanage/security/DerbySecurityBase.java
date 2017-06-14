package com.linbit.drbdmanage.security;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public abstract class DerbySecurityBase extends DerbyBase
{
    protected static final String VIEW_SEC_IDENTITIES_LOAD = "SEC_IDENTITIES_LOAD";
    protected static final String VIEW_SEC_ROLES_LOAD = "SEC_ROLES_LOAD";
    protected static final String VIEW_SEC_TYPES_LOAD = "SEC_TYPES_LOAD";
    protected static final String VIEW_SEC_TYPE_RULES_LOAD = "SEC_TYPE_RULES_LOAD";

    protected static final String TBL_SEC_CONFIGURATION = "SEC_CONFIGURATION";
    protected static final String TBL_SEC_IDENTITIES = "SEC_IDENTITIES";
    protected static final String TBL_SEC_TYPES = "SEC_TYPES";
    protected static final String TBL_SEC_ROLES = "SEC_ROLES";
    protected static final String TBL_SEC_ID_ROLE_MAP = "SEC_ID_ROLE_MAP";
    protected static final String TBL_SEC_ACCESS_TYPES = "SEC_ACCESS_TYPES";
    protected static final String TBL_SEC_TYPE_RULES = "SEC_TYPE_RULES";
    protected static final String TBL_SEC_DFLT_ROLES = "SEC_DFLT_ROLES";
    protected static final String TBL_SEC_OBJECT_PROTECTION = "SEC_OBJECT_PROTECTION";
    protected static final String TBL_SEC_ACL_MAP = "SEC_ACL_MAP";

    protected static final String SEC_CONFIG_ENTRY_KEY = "ENTRY_KEY";
    protected static final String SEC_CONFIG_ENTRY_DSP_KEY = "ENTRY_DSP_KEY";
    protected static final String SEC_CONFIG_ENTRY_VALUE = "ENTRY_VALUE";

    protected static final String ID_NAME = "IDENTITY_NAME";
    protected static final String ID_DSP_NAME = "IDENTITY_DSP_NAME";
    protected static final String ID_PASS_SALT = "PASS_SALT";
    protected static final String ID_PASS_HASH = "PASS_HASH";
    protected static final String ID_ENABLED = "ID_ENABLED";
    protected static final String ID_LOCKED = "ID_LOCKED";

    protected static final String TYPE_NAME = "TYPE_NAME";
    protected static final String TYPE_DSP_NAME = "TYPE_DSP_NAME";
    protected static final String TYPE_ENABLED = "TYPE_ENABLED";

    protected static final String ROLE_NAME = "ROLE_NAME";
    protected static final String ROLE_DSP_NAME = "ROLE_DSP_NAME";
    protected static final String DOMAIN_NAME = "DOMAIN_NAME";
    protected static final String ROLE_ENABLED = "ROLE_ENABLED";
    protected static final String ROLE_PRIVILEGES = "ROLE_PRIVILEGES";

    protected static final String ACCESS_TYPE_NAME = "ACCESS_TYPE_NAME";
    protected static final String ACCESS_TYPE_VALUE = "ACCESS_TYPE_VALUE";
    protected static final String ACCESS_TYPE = "ACCESS_TYPE";

    protected static final String OBJECT_PATH = "OBJECT_PATH";
    protected static final String CREATOR_IDENTITY_NAME = "CREATOR_IDENTITY_NAME";
    protected static final String OWNER_ROLE_NAME = "OWNER_ROLE_NAME";
    protected static final String SECURITY_TYPE_NAME = "SECURITY_TYPE_NAME";

    private static final String[] CREATE_TABLES = new String[]
        {
            "CREATE TABLE " + TBL_SEC_CONFIGURATION + " \n" +
            "( \n" +
            "   " + SEC_CONFIG_ENTRY_KEY + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
            "       CONSTRAINT SEC_CONF_CHKKEY CHECK (UPPER(" + SEC_CONFIG_ENTRY_KEY + ") = " + SEC_CONFIG_ENTRY_KEY + " AND LENGTH(" + SEC_CONFIG_ENTRY_KEY + ") >= 3), \n" +
            "   " + SEC_CONFIG_ENTRY_DSP_KEY + " VARCHAR(24) NOT NULL, \n" +
            "   " + SEC_CONFIG_ENTRY_VALUE + " VARCHAR(24) NOT NULL, \n" +
            "       CONSTRAINT SEC_CONF_CHKDSPKEY CHECK (UPPER(" + SEC_CONFIG_ENTRY_DSP_KEY + ") = " + SEC_CONFIG_ENTRY_KEY + ") \n" +
            ")",
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
            ")",
            "CREATE TABLE " + TBL_SEC_TYPES + " \n" +
            "( \n" +
            "    " + TYPE_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
            "        CONSTRAINT SEC_TYPES_CHKNAME CHECK (UPPER(" + TYPE_NAME + ") = " + TYPE_NAME + " AND LENGTH(" + TYPE_NAME + ") >= 3), \n" +
            "    " + TYPE_DSP_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    " + TYPE_ENABLED + " BOOLEAN NOT NULL DEFAULT TRUE, \n" +
            "    CONSTRAINT SEC_TYPES_CHKDSPNAME CHECK (UPPER(" + TYPE_DSP_NAME + ") = " + TYPE_NAME + ") \n" +
            ")",
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
            ")",
            "CREATE TABLE " + TBL_SEC_ID_ROLE_MAP + " \n" +
            "( \n" +
            "    " + ID_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    " + ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    PRIMARY KEY (" + ID_NAME + ", " + ROLE_NAME + "), \n" +
            "    FOREIGN KEY (" + ID_NAME + ") REFERENCES " + TBL_SEC_IDENTITIES + "(" + ID_NAME + ") ON DELETE CASCADE, \n" +
            "    FOREIGN KEY (" + ROLE_NAME + ") REFERENCES " + TBL_SEC_ROLES + "(" + ROLE_NAME + ") ON DELETE CASCADE \n" +
            ")",
            "CREATE TABLE " + TBL_SEC_ACCESS_TYPES + " \n" +
            "( \n" +
            "    " + ACCESS_TYPE_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY \n" +
            "        CONSTRAINT SEC_ACCESS_TYPES_CHKNAME CHECK (UPPER(" + ACCESS_TYPE_NAME + ") = " + ACCESS_TYPE_NAME + "), \n" +
            "    " + ACCESS_TYPE_VALUE + " SMALLINT NOT NULL UNIQUE \n" +
            ")",
            "CREATE TABLE " + TBL_SEC_TYPE_RULES + " \n" +
            "( \n" +
            "    " + DOMAIN_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    " + TYPE_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    " + ACCESS_TYPE + " SMALLINT NOT NULL, \n" +
            "    PRIMARY KEY (" + DOMAIN_NAME + ", " + TYPE_NAME + "), \n" +
            "    FOREIGN KEY (" + DOMAIN_NAME + ") REFERENCES " + TBL_SEC_TYPES + "(" + TYPE_NAME + ") ON DELETE CASCADE, \n" +
            "    FOREIGN KEY (" + TYPE_NAME + ") REFERENCES " + TBL_SEC_TYPES + "(" + TYPE_NAME + ") ON DELETE CASCADE, \n" +
            "    FOREIGN KEY (" + ACCESS_TYPE + ") REFERENCES " + TBL_SEC_ACCESS_TYPES + "(" + ACCESS_TYPE_VALUE + ") ON DELETE RESTRICT \n" +
            ")",
            "CREATE TABLE " + TBL_SEC_DFLT_ROLES + " \n" +
            "( \n" +
            "    " + ID_NAME + " VARCHAR(24) NOT NULL PRIMARY KEY, \n" +
            "    " + ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    FOREIGN KEY (" + ID_NAME + ", " + ROLE_NAME + ") REFERENCES " + TBL_SEC_ID_ROLE_MAP + "(" + ID_NAME + ", " + ROLE_NAME + ") \n" +
            "        ON DELETE CASCADE \n" +
            ")",
            "CREATE TABLE " + TBL_SEC_OBJECT_PROTECTION + " \n" +
            "( \n" +
            "    " + OBJECT_PATH + " VARCHAR(512) NOT NULL PRIMARY KEY, \n" +
            "    " + CREATOR_IDENTITY_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    " + OWNER_ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    " + SECURITY_TYPE_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    FOREIGN KEY (" + CREATOR_IDENTITY_NAME + ") REFERENCES " + TBL_SEC_IDENTITIES + "(" + ID_NAME + ") ON DELETE RESTRICT, \n" +
            "    FOREIGN KEY (" + OWNER_ROLE_NAME + ") REFERENCES " + TBL_SEC_ROLES + "(" + ROLE_NAME + ") ON DELETE RESTRICT, \n" +
            "    FOREIGN KEY (" + SECURITY_TYPE_NAME + ") REFERENCES " + TBL_SEC_TYPES + "(" + TYPE_NAME + ") ON DELETE RESTRICT \n" +
            ")",
            "CREATE TABLE " + TBL_SEC_ACL_MAP + " \n" +
            "( \n" +
            "    " + OBJECT_PATH + " VARCHAR(512) NOT NULL, \n" +
            "    " + ROLE_NAME + " VARCHAR(24) NOT NULL, \n" +
            "    " + ACCESS_TYPE + " SMALLINT NOT NULL, \n" +
            "    PRIMARY KEY (" + OBJECT_PATH + ", " + ROLE_NAME + "), \n" +
            "    FOREIGN KEY (" + OBJECT_PATH + ") REFERENCES " + TBL_SEC_OBJECT_PROTECTION + "(" + OBJECT_PATH + ") ON DELETE CASCADE, \n" +
            "    FOREIGN KEY (" + ROLE_NAME + ") REFERENCES " + TBL_SEC_ROLES + "(" + ROLE_NAME + ") ON DELETE RESTRICT, \n" +
            "    FOREIGN KEY (" + ACCESS_TYPE + ") REFERENCES " + TBL_SEC_ACCESS_TYPES + "(" + ACCESS_TYPE_VALUE + ") ON DELETE RESTRICT \n" +
            ")",
            "CREATE VIEW " + VIEW_SEC_IDENTITIES_LOAD + " AS " +
            "    SELECT " + ID_DSP_NAME + ", " + ID_ENABLED + " " +
            "    FROM " + TBL_SEC_IDENTITIES,
            "CREATE VIEW " + VIEW_SEC_ROLES_LOAD + " AS " +
            "    SELECT " + ROLE_DSP_NAME + ", " + ROLE_ENABLED +
            "    FROM " + TBL_SEC_ROLES,
            "CREATE VIEW " + VIEW_SEC_TYPES_LOAD + " AS " +
            "    SELECT " + TYPE_DSP_NAME + ", " + TYPE_ENABLED +
            "    FROM " + TBL_SEC_TYPES,
            "CREATE VIEW " + VIEW_SEC_TYPE_RULES_LOAD + " AS " +
            "    SELECT " + DOMAIN_NAME + ", " + TYPE_NAME + ", " + TBL_SEC_ACCESS_TYPES + "." + ACCESS_TYPE_NAME + " " + "AS " + ACCESS_TYPE +
            "    FROM " + TBL_SEC_TYPE_RULES +
            "    LEFT JOIN " + TBL_SEC_ACCESS_TYPES +
            "        ON " + TBL_SEC_TYPE_RULES + "." + ACCESS_TYPE + " = " + TBL_SEC_ACCESS_TYPES + "." + ACCESS_TYPE_VALUE +
            "    ORDER BY " + DOMAIN_NAME + ", " + TYPE_NAME + " ASC"
        };
    private static final String[] DEFAULT_VALUES = new String[]
        {
            "INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
                " VALUES ('CONTROL', 15)",
            "INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
                " VALUES ('CHANGE', 7)",
            "INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
                " VALUES ('USE', 3)",
            "INSERT INTO " + TBL_SEC_ACCESS_TYPES + " (" + ACCESS_TYPE_NAME + ", " + ACCESS_TYPE_VALUE + ") " +
                " VALUES ('VIEW', 1)",

            "INSERT INTO " + TBL_SEC_IDENTITIES + " (" + ID_NAME + ", " + ID_DSP_NAME + ", " + ID_ENABLED + ", " + ID_LOCKED + ") " +
                " VALUES('SYSTEM', 'SYSTEM', TRUE, TRUE)",
            "INSERT INTO " + TBL_SEC_IDENTITIES + " (" + ID_NAME + ", " + ID_DSP_NAME + ", " + ID_ENABLED + ", " + ID_LOCKED + ") " +
                " VALUES('PUBLIC', 'PUBLIC', TRUE, TRUE)",

            "INSERT INTO " + TBL_SEC_TYPES + " (" + TYPE_NAME + ", " + TYPE_DSP_NAME + ", " + TYPE_ENABLED + ") " +
                " VALUES ('SYSTEM', 'SYSTEM', TRUE)",
            "INSERT INTO " + TBL_SEC_TYPES + " (" + TYPE_NAME + ", " + TYPE_DSP_NAME + ", " + TYPE_ENABLED + ") " +
                " VALUES ('PUBLIC', 'PUBLIC', TRUE)",

            "INSERT INTO " + TBL_SEC_ROLES + " (" + ROLE_NAME + ", " + ROLE_DSP_NAME + ", " + DOMAIN_NAME + ", " + ROLE_ENABLED + ", " + ROLE_PRIVILEGES + ") " +
                " VALUES('SYSTEM', 'SYSTEM', 'SYSTEM', TRUE, -9223372036854775808)",
            "INSERT INTO " + TBL_SEC_ROLES + " (" + ROLE_NAME + ", " + ROLE_DSP_NAME + ", " + DOMAIN_NAME + ", " + ROLE_ENABLED + ", " + ROLE_PRIVILEGES + ") " +
                " VALUES('PUBLIC', 'PUBLIC', 'PUBLIC', TRUE, 0)",

            "INSERT INTO " + TBL_SEC_ID_ROLE_MAP + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
                " VALUES ('SYSTEM', 'SYSTEM')",
            "INSERT INTO " + TBL_SEC_ID_ROLE_MAP + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
                " VALUES ('PUBLIC', 'PUBLIC')",

            "INSERT INTO " + TBL_SEC_DFLT_ROLES + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
                " VALUES ('SYSTEM', 'SYSTEM')",
            "INSERT INTO " + TBL_SEC_DFLT_ROLES + " (" + ID_NAME + ", " + ROLE_NAME + ") " +
                " VALUES ('PUBLIC', 'PUBLIC')",

            "INSERT INTO SEC_CONFIGURATION (ENTRY_KEY, ENTRY_DSP_KEY, ENTRY_VALUE) VALUES ('SECURITYLEVEL', 'SecurityLevel', 'MAC')",
        };
    private static final String[] DROP_TABLES = new String[]
        {
            "DROP VIEW " + VIEW_SEC_TYPE_RULES_LOAD,
            "DROP VIEW " + VIEW_SEC_TYPES_LOAD,
            "DROP VIEW " + VIEW_SEC_ROLES_LOAD,
            "DROP VIEW " + VIEW_SEC_IDENTITIES_LOAD,
            "DROP TABLE " + TBL_SEC_ACL_MAP,
            "DROP TABLE " + TBL_SEC_OBJECT_PROTECTION,
            "DROP TABLE " + TBL_SEC_DFLT_ROLES,
            "DROP TABLE " + TBL_SEC_TYPE_RULES,
            "DROP TABLE " + TBL_SEC_ACCESS_TYPES,
            "DROP TABLE " + TBL_SEC_ID_ROLE_MAP,
            "DROP TABLE " + TBL_SEC_ROLES,
            "DROP TABLE " + TBL_SEC_TYPES,
            "DROP TABLE " + TBL_SEC_IDENTITIES,
            "DROP TABLE " + TBL_SEC_CONFIGURATION
        };
    private static final String[] TRUNCATE_TABLES = new String[]
        {
            "DELETE FROM " + TBL_SEC_ACL_MAP + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_OBJECT_PROTECTION + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_DFLT_ROLES + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_TYPE_RULES + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_ACCESS_TYPES + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_ID_ROLE_MAP + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_ROLES + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_TYPES + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_IDENTITIES + " WHERE 1=1",
            "DELETE FROM " + TBL_SEC_CONFIGURATION + " WHERE 1=1"
        };


    public DerbySecurityBase() throws SQLException
    {
        super(CREATE_TABLES, DEFAULT_VALUES, TRUNCATE_TABLES, DROP_TABLES);
    }

    protected void createTables2(Connection con, boolean dropIfExists) throws SQLException
    {
        for (int idx = 0; idx < CREATE_TABLES.length; ++idx)
        {
            createTable(con, dropIfExists, idx);
        }
        for (String insert : DEFAULT_VALUES)
        {
            try (PreparedStatement stmt = con.prepareStatement(insert))
            {
                stmt.executeUpdate();
            }
        }
    }

    protected void dropTables2(Connection con) throws SQLException
    {
        for (int idx = 0; idx < DROP_TABLES.length; ++idx)
        {
            dropTable(con, idx);
        }
    }

    private void createTable(Connection con, boolean dropIfExists, int idx) throws SQLException
    {
        try
        {
            try (PreparedStatement stmt = con.prepareStatement(CREATE_TABLES[idx]))
            {
                stmt.executeUpdate();
            }
        }
        catch (SQLException sqlExc)
        {
            String sqlState = sqlExc.getSQLState();
            if ("X0Y32".equals(sqlState)) // table already exists
            {
                if (dropIfExists)
                {
                    dropTable(con, DROP_TABLES.length - 1 - idx);
                    createTable(con, false, idx);
                }
                else
                {
                    throw sqlExc;
                }
            }
            else
            {
                throw sqlExc;
            }
        }
        con.commit();
    }

    private void dropTable(Connection con, int idx) throws SQLException
    {
        try (PreparedStatement stmt = con.prepareStatement(DROP_TABLES[idx]))
        {
            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            if ("42Y55".equals(sqlExc.getSQLState()))
            {
                // table does not exists.... yay - ignore
            }
            else
            {
                throw sqlExc;
            }
        }
        con.commit();
    }
}
