package com.linbit.linstor.dbcp.migration;

import com.google.common.io.Resources;
import com.linbit.linstor.dbdrivers.derby.DbConstants;

import com.linbit.ImplementationError;
import com.linbit.linstor.DatabaseInfo;
import com.linbit.linstor.DatabaseInfo.DbProduct;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MigrationUtils
{
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
        String defaultValRef
    )
    {
        StringBuilder sql = new StringBuilder();

        String type = getDialectType(database, typeRef);
        database.name();
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
                    sql.append(" DEFAULT '").append(defaultValRef).append("'");
                }
                sql.append(";");
                break;
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + database);
        }

        return sql.toString();
    }

    public static String getDialectType(DbProduct databaseRef, String typeRef)
    {
        String type;
        switch (databaseRef)
        {
            case ASE:
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
            case MSFT_SQLSERVER:
                type = typeRef.replaceAll("BLOB", "BINARY");
                break;
            case INFORMIX:
                type = typeRef.replaceAll("VARCHAR", "LVARCHAR");
                break;
            case ORACLE_RDBMS:
                type = typeRef.replaceAll("VARCHAR", "VARCHAR2");
                break;
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
                    getDialectType(databaseRef, typeRef)
                );
                break;
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
                sql = String.format("ALTER TABLE %s DROP FOREIGN KEY %s;", table, constraintName);
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
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }

    public static String dropColumnConstraint(
        DbProduct dbProductRef,
        String table,
        String constraintName
    )
    {
        String sql = null;
        switch (dbProductRef)
        {
            case MARIADB:
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
            case UNKNOWN:
            default:
                throw new ImplementationError("Unexpected database type: " + dbProductRef);
        }
        return sql;
    }
}
