package com.linbit.linstor.dbcp.migration;

import com.linbit.ImplementationError;
import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.DatabaseInfo.DbProduct;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.utils.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

import com.google.common.io.Resources;

public class MigrationUtils
{
    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)(?:\\.(\\d+))");

    public static final String META_COL_TABLE_NAME = "TABLE_NAME";
    public static final String META_COL_COLUMN_NAME = "COLUMN_NAME";

    public static String loadResource(String resourceName)
        throws IOException
    {
        return Resources.toString(Resources.getResource(MigrationUtils.class, resourceName), StandardCharsets.UTF_8);
    }

    public static boolean tableExists(Connection connection, String tableName)
        throws SQLException
    {
        boolean exists = false;

        DatabaseMetaData metaData = connection.getMetaData();

        // Fetch all tables in order to do a case-insensitive search
        ResultSet res = metaData.getTables(null, DbConstants.DATABASE_SCHEMA_NAME, null, null);

        while (res.next())
        {
            String resTableName = res.getString(META_COL_TABLE_NAME);
            if (tableName.equalsIgnoreCase(resTableName))
            {
                exists = true;
            }
        }

        return exists;
    }

    public static boolean columnExists(Connection connection, String tableName, String columnName)
        throws SQLException
    {
        boolean exists = false;

        DatabaseMetaData metaData = connection.getMetaData();

        // Fetch all columns in order to do a case-insensitive search
        ResultSet res = metaData.getColumns(null, DbConstants.DATABASE_SCHEMA_NAME, null, null);

        while (res.next())
        {
            String resTableName = res.getString(META_COL_TABLE_NAME);
            String resColumnName = res.getString(META_COL_COLUMN_NAME);
            if (tableName.equalsIgnoreCase(resTableName) && columnName.equalsIgnoreCase(resColumnName))
            {
                exists = true;
            }
        }

        return exists;
    }

    public static String dropColumn(DatabaseInfo.DbProduct database, String table, String column)
    {
        String sql;
        switch (database)
        {
            case ASE:
                sql = String.format("ALTER TABLE %s DROP %s;", table, column);
                break;
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case INFORMIX:
            case MARIADB:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql = String.format("ALTER TABLE %s DROP COLUMN %s;", table, column);
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + database);

        }
        return sql;
    }

    public static String addColumn(
        DatabaseInfo.DbProduct database,
        String table,
        String column,
        String typeRef,
        boolean nullable,
        @Nullable String defaultValRef,
        @Nullable String afterColumn
    )
    {
        StringBuilder sql = new StringBuilder();

        String type = replaceTypesByDialect(database, typeRef);

        String defaultVal = null;
        if (defaultValRef != null)
        {
            if (!typeRef.equals("BOOL") && !typeRef.equals("BOOLEAN"))
            {
                defaultVal = "'" + defaultValRef + "'";
            }
            else
            {
                defaultVal = defaultValRef;
            }

        }
        switch (database)
        {
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case INFORMIX:
            case MARIADB:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql.append("ALTER TABLE ").append(table)
                    .append(" ADD ").append(column).append(" ").append(type);
                if (!nullable)
                {
                    sql.append(" NOT");
                }
                sql.append(" NULL");
                if (defaultValRef != null)
                {
                    sql.append(" DEFAULT ").append(defaultVal);
                }
                if (database.equals(DbProduct.H2) && afterColumn != null) // we only need this for our own H2 setup
                {
                    sql.append(" AFTER ").append(afterColumn);
                }
                sql.append(";");
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + database);
        }

        return sql.toString();
    }

    public static String replaceTypesByDialect(DbProduct databaseRef, String typeRef)
    {
        String type;
        switch (databaseRef)
        {
            case ASE:
            case MSFT_SQLSERVER:
                type = typeRef.replaceAll("BLOB", "BINARY");
                break;
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case MARIADB:
            case MYSQL:
                type = typeRef;
                break;
            case POSTGRESQL:
                type = typeRef.replaceAll("BLOB", "BYTEA");
                break;
            case INFORMIX:
                type = typeRef.replaceAll("VARCHAR", "LVARCHAR");
                break;
            case ORACLE_RDBMS:
                type = typeRef.replaceAll("VARCHAR", "VARCHAR2");
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + databaseRef);
        }
        return type;
    }

    private MigrationUtils()
    {
    }

    public static String addColumnConstraintNotNull(
        DbProduct databaseRef,
        String table,
        String column,
        String typeRef
    )
    {
        String sql;
        switch (databaseRef)
        {
            case ASE:
            case INFORMIX:
            case ORACLE_RDBMS:
                sql = String.format("ALTER TABLE %s MODIFY %s SET NOT NULL;", table, column);
                break;
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case MSFT_SQLSERVER:
            case POSTGRESQL:
                sql = String.format("ALTER TABLE %s ALTER %s SET NOT NULL;", table, column);
                break;
            case MARIADB:
            case MYSQL:
                sql = String.format(
                    "ALTER TABLE %s MODIFY COLUMN %s %s NOT NULL;",
                    table,
                    column,
                    replaceTypesByDialect(databaseRef, typeRef)
                );
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + databaseRef);
        }
        return sql;
    }

    public static String dropColumnConstraintNotNull(
        DbProduct databaseRef,
        String table,
        String column,
        String columnDefinition  // column type definition without the NOT NULL clause
    )
    {
        String sql;
        switch (databaseRef)
        {
            case ASE:
            case INFORMIX:
            case ORACLE_RDBMS:
                sql = String.format("ALTER TABLE %s MODIFY %s DROP NOT NULL", table, column);
                break;
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case POSTGRESQL:
                sql = String.format("ALTER TABLE %s ALTER %s DROP NOT NULL;", table, column);
                break;
            case MSFT_SQLSERVER:
                sql = String.format("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", table, column);
                break;
            case MARIADB:
            case MYSQL:
                sql = String.format(
                    "ALTER TABLE %s MODIFY COLUMN %s %s;",
                    table,
                    column,
                    columnDefinition
                );
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + databaseRef);
        }
        return sql;
    }

    public static String dropForeignKeyConstraint(
        DbProduct dbProduct,
        String table,
        String fkName
    )
    {
        String sql;
        switch (dbProduct)
        {
            case ASE:
            case INFORMIX:
            case ORACLE_RDBMS:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case POSTGRESQL:
            case MSFT_SQLSERVER:
                sql = String.format(
                    "ALTER TABLE %s DROP CONSTRAINT %s;",
                    table,
                    fkName
                );
                break;
            case MARIADB:
            case MYSQL:
                sql = String.format(
                    "ALTER TABLE %s DROP FOREIGN KEY %s;",
                    table,
                    fkName
                );
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProduct);
        }
        return sql;
    }

    public static String dropColumnConstraintForeignKey(
        DbProduct dbProductRef,
        String table,
        String constraintName
    )
    {
        String sql = null;
        switch (dbProductRef)
        {
            case MARIADB:
                sql = String.format(
                    "ALTER TABLE %s DROP FOREIGN KEY %s;\n" +
                        "ALTER TABLE %s DROP KEY IF EXISTS %s;",
                    table, constraintName,
                    table, constraintName
                );
                break;
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case INFORMIX:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql = String.format("ALTER TABLE %s DROP CONSTRAINT %s;", table, constraintName);
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String dropColumnConstraintCheck(
        Connection dbConRef,
        DbProduct dbProductRef,
        String table,
        String constraintName
    ) throws SQLException, ImplementationError
    {
        String sql = null;
        switch (dbProductRef)
        {
            case MARIADB:
                sql = String.format("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s;", table, constraintName);
                break;
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case INFORMIX:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql = String.format("ALTER TABLE %s DROP CONSTRAINT %s;", table, constraintName);
                break;
            case ETCD: // fall-through
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String addColumnConstraintCheck(
        DbProduct dbProductRef,
        String tableName,
        String checkName,
        String checkCondition
    )
    {
        String sql;
        switch (dbProductRef)
        {
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case MARIADB:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql = String.format(
                    "ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
                    tableName,
                    checkName,
                    checkCondition
                );
                break;
            case INFORMIX:
                sql = String.format(
                    "ALTER TABLE %s ADD CONSTRAINT CHECK (%s) CONSTRAINT %s",
                    tableName,
                    checkCondition,
                    checkName
                );
                break;
            case ETCD:
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String dropColumnConstraintPrimaryKey(DbProduct dbProductRef, String tableName, String pkName)
    {
        String sql;
        switch (dbProductRef)
        {
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case INFORMIX:
            case MARIADB:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
                sql = String.format("ALTER TABLE %s DROP PRIMARY KEY", tableName);
                break;
            case POSTGRESQL:
                sql = String.format("ALTER TABLE %s DROP CONSTRAINT %s", tableName, pkName);
                break;
            case ETCD:
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String dropTable(DbProduct dbProductRef, String tblNameRef)
    {
        String sql;
        switch (dbProductRef)
        {
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case MSFT_SQLSERVER:
                sql = String.format("DROP TABLE %s", tblNameRef);
                break;
            case H2:
            case INFORMIX:
            case MARIADB:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql = String.format("DROP TABLE IF EXISTS %s", tblNameRef);
                break;
            case ETCD:
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String addColumnConstraintPrimaryKey(
        DbProduct dbProductRef,
        String tableName,
        String constraintName,
        String... columns
    )
    {
        String sql;
        String joinedColumns = StringUtils.join(", ", columns);
        switch (dbProductRef)
        {
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case MARIADB:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql = String.format(
                    "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)",
                    tableName,
                    constraintName,
                    joinedColumns
                );
                break;
            case INFORMIX:
                sql = String.format(
                    "ALTER TABLE %s ADD CONSTRAINT PRIMARY KEY (%s) CONSTRAINT %s",
                    tableName,
                    joinedColumns,
                    constraintName
                );
                break;
            case ETCD:
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String addColumnConstraintForeignKey(
        DbProduct dbProductRef,
        String localTableName,
        String fkName,
        String joinedLocalColumns,
        String remoteTableName,
        String joinedRemoteColumns
    )
    {
        String sql;
        switch (dbProductRef)
        {
            case ASE:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case H2:
            case MARIADB:
            case MSFT_SQLSERVER:
            case MYSQL:
            case ORACLE_RDBMS:
            case POSTGRESQL:
                sql = String.format(
                    "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s)",
                    localTableName,
                    fkName,
                    joinedLocalColumns,
                    remoteTableName,
                    joinedRemoteColumns
                );
                break;
            case INFORMIX:
                sql = String.format(
                    "ALTER TABLE %s ADD CONSTRAINT FOREIGN KEY (%s) REFERENCES %s (%s) CONSTRAINT %s",
                    localTableName,
                    joinedLocalColumns,
                    remoteTableName,
                    joinedRemoteColumns,
                    fkName
                );
                break;
            case UNKNOWN:
                // fall-through
            case ETCD:
                // fall-through
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String alterColumnType(DbProduct dbProductRef, String table, String col, String newType)
    {
        String sql;
        String type = replaceTypesByDialect(dbProductRef, newType);
        switch (dbProductRef)
        {
            case ASE:
            case INFORMIX:
            case ORACLE_RDBMS:
                sql = String.format("ALTER TABLE %s MODIFY %s %s", table, col, type);
                break;
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
                sql = String.format("ALTER TABLE %s ALTER %s SET DATA TYPE %s", table, col, type);
                break;
            case H2:
                sql = String.format("ALTER TABLE %s ALTER %s %s", table, col, type);
                break;
            case MARIADB:
            case MYSQL:
                sql = String.format("ALTER TABLE %s CHANGE COLUMN %s %s %s", table, col, col, type);
                break;
            case MSFT_SQLSERVER:
                sql = String.format("ALTER TABLE %s ALTER COLUMN %s %s", table, col, type);
                break;
            case POSTGRESQL:
                sql = String.format("ALTER TABLE %s ALTER %s TYPE %s", table, col, type);
                break;
            case ETCD:
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);

        }
        return sql;
    }

    /**
     * Caution, this method does not return a full SQL statement, but only the sql-dialect version of a concat.
     * Also make sure to quote ALL constant strings, otherwise this method could not reference columns!
     * example: concat("\"prefix\"", "NODE_NAME", "\"suffix\"");
     *
     * @param dbProductRef
     * @param concatParams
     *
     * @return
     */
    public static String concat(DbProduct dbProductRef, String... concatParams)
    {
        String prefix = null;
        String glue = ", ";
        String suffix = null;

        switch (dbProductRef)
        {
            case ASE:
            case H2:
            case POSTGRESQL:
            case DB2:
            case DB2_I:
            case DB2_Z:
            case DERBY:
            case INFORMIX:
            case ORACLE_RDBMS:
                prefix = "(";
                suffix = ")";
                glue = " || ";
                break;
            case MSFT_SQLSERVER:
                prefix = "(";
                suffix = ")";
                glue = " + ";
                break;
            case MARIADB:
            case MYSQL:
                prefix = "concat(";
                suffix = ")";
                glue = ", ";
                break;
            case ETCD:
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        StringBuilder sb = new StringBuilder();
        if (prefix != null)
        {
            sb.append(prefix);
        }
        for (String concatParam : concatParams)
        {
            sb.append(concatParam).append(glue);
        }
        sb.setLength(sb.length() - glue.length());
        if (suffix != null)
        {
            sb.append(suffix);
        }

        return sb.toString();
    }
}
