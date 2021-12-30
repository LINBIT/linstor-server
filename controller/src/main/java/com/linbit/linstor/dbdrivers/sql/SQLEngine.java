package com.linbit.linstor.dbdrivers.sql;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver.RawParameters;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.sql.dump.DbDump;
import com.linbit.linstor.dbdrivers.sql.dump.SqlDump;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.manager.TransactionMgrSQL;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.inject.Provider;

@Singleton
public class SQLEngine implements DbEngine
{
    private static final String DELIMITER_AND = "AND ";
    private static final String DELIMITER_EQUALS_AND = " = ? " + DELIMITER_AND;
    private static final String DELIMITER_LIST = ", ";
    private static final String DELIMITER_VALUES = "?" + DELIMITER_LIST;

    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final HashMap<DatabaseTable, String> selectStatements;
    private final HashMap<DatabaseTable, String> insertStatements;
    private final HashMap<DatabaseTable, String> deleteStatements;
    private final CtrlConfig ctrlCfg;

    @Inject
    public SQLEngine(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrSQL> transMgrProviderRef,
        CtrlConfig ctrlCfgRef
    )
    {
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
        ctrlCfg = ctrlCfgRef;

        selectStatements = new HashMap<>();
        insertStatements = new HashMap<>();
        deleteStatements = new HashMap<>();
    }

    @Override
    public DatabaseType getType()
    {
        return DatabaseType.SQL;
    }

    @Override
    public ApiCallRc backupDb(String backupPath) throws DatabaseException
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        if (ctrlCfg.getDbConnectionUrl().toLowerCase().startsWith("jdbc:h2"))
        {
            try (PreparedStatement stmt = getConnection().prepareStatement(String.format("BACKUP TO '%s'", backupPath)))
            {
                stmt.execute();
                apiCallRc.addEntry(
                    "Database backup created: " + backupPath, ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT);
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(sqlExc);
            }
        }
        else
        {
            apiCallRc.addEntry(
                "Only h2 database is currently supported for online backup.", ApiConsts.FAIL_UNKNOWN_ERROR);
        }

        return apiCallRc;
    }

    @Override
    public <DATA> void create(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        DatabaseTable table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(getInsertStatement(table)))
        {
            errorReporter.logTrace("Creating %s %s", table.getName(), dataToString.toString(data));

            setValues(setters, stmt, 1, table, ignored -> true, data);

            stmt.executeUpdate();

            errorReporter.logTrace("%s created %s", table.getName(), dataToString.toString(data));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException exc)
        {
            DatabaseLoader.handleAccessDeniedException(exc);
        }
    }

    private String getSelectStatement(DatabaseTable table)
    {
        String sql = selectStatements.get(table);
        if (sql == null)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT ");
            for (Column col : table.values())
            {
                sqlBuilder.append(col.getName()).append(DELIMITER_LIST);
            }
            sqlBuilder.setLength(sqlBuilder.length() - DELIMITER_LIST.length());
            sqlBuilder.append(" FROM ").append(table.getName());

            sql = sqlBuilder.toString();
            selectStatements.put(table, sql);
        }
        return sql;
    }

    private String getInsertStatement(DatabaseTable table)
    {
        String sql = insertStatements.get(table);
        if (sql == null)
        {
            StringBuilder values = new StringBuilder();
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("INSERT INTO ").append(table.getName()).append(" (");
            for (Column col : table.values())
            {
                sqlBuilder.append(col.getName()).append(DELIMITER_LIST);
                values.append(DELIMITER_VALUES);
            }
            sqlBuilder.setLength(sqlBuilder.length() - DELIMITER_LIST.length());
            values.setLength(values.length() - DELIMITER_LIST.length());

            sqlBuilder.append(") VALUES(").append(values).append(")");

            sql = sqlBuilder.toString();
            insertStatements.put(table, sql);
        }
        return sql;
    }

    @Override
    public <DATA> void delete(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        DatabaseTable table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(getDeleteStatement(table)))
        {
            errorReporter.logTrace("Deleting %s %s", table.getName(), dataToString.toString(data));

            setPrimaryValues(setters, stmt, 1, table, data);

            stmt.executeUpdate();

            errorReporter.logTrace("%s deleted %s", table.getName(), dataToString.toString(data));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException exc)
        {
            DatabaseLoader.handleAccessDeniedException(exc);
        }
    }

    private String getDeleteStatement(DatabaseTable table)
    {
        String sql = deleteStatements.get(table);
        if (sql == null)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ").append(table.getName()).append(" WHERE ");
            for (Column col : table.values())
            {
                if (col.isPk())
                {
                    sqlBuilder.append(col.getName()).append(DELIMITER_EQUALS_AND);
                }
            }
            sqlBuilder.setLength(sqlBuilder.length() - DELIMITER_AND.length());

            sql = sqlBuilder.toString();
            deleteStatements.put(table, sql);
        }
        return sql;
    }


    @Override
    public <DATA, FLAG extends Enum<FLAG> & Flags> StateFlagsPersistence<DATA> generateFlagsDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column col,
        Class<FLAG> flagsClass,
        DataToString<DATA> dataToString
    )
    {
        return new SQLFlagsDriver<>(
            this,
            errorReporter,
            setters,
            col,
            flagsClass,
            dataToString
        );
    }

    @Override
    public <DATA, INPUT_TYPE, DB_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateSingleColumnDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        Function<INPUT_TYPE, DB_TYPE> typeMapperRef,
        DataToString<DATA> dataToStringRef,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToStringRef
    )
    {
        return new SQLSingleColumnDriver<>(
            this,
            errorReporter,
            setters,
            colRef,
            typeMapperRef,
            dataToStringRef,
            dataValueToStringRef
        );
    }

    @Override
    public <DATA, LIST_TYPE> CollectionDatabaseDriver<DATA, LIST_TYPE> generateCollectionToJsonStringArrayDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        DataToString<DATA> dataToStringRef
    )
    {
        return new SQLListToJsonArrayDriver<>(this, errorReporter, setters, colRef, dataToStringRef);
    }

    @Override
    public <DATA, INIT_MAPS, LOAD_ALL> Map<DATA, INIT_MAPS> loadAll(
        DatabaseTable table,
        LOAD_ALL parentsRef,
        DataLoader<DATA, INIT_MAPS, LOAD_ALL> dataLoaderRef
    )
        throws DatabaseException, AccessDeniedException, MdException
    {
        Map<DATA, INIT_MAPS> loadedObjectsMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(getSelectStatement(table)))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Pair<DATA, INIT_MAPS> pair = restoreData(
                        table, resultSet, parentsRef, dataLoaderRef
                    );
                    // pair might be null when loading objects sharing the same table.
                    // For example SnapshotDbDriver will return null when finding a Resource entry
                    // and vice versa.
                    if (pair != null)
                    {
                        loadedObjectsMap.put(pair.objA, pair.objB);
                    }
                }
            }
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
        return loadedObjectsMap;
    }

    private <DATA, INIT_MAPS, LOAD_ALL> Pair<DATA, INIT_MAPS> restoreData(
        DatabaseTable table,
        ResultSet resultSet,
        LOAD_ALL parents,
        DataLoader<DATA, INIT_MAPS, LOAD_ALL> dataLoader
    )
        throws DatabaseException, MdException
    {
        Column[] columns = table.values();
        Map<String, Object> objects = new TreeMap<>();
        try
        {
            for (Column column : columns)
            {
                Object data;
                int colSqlType = column.getSqlType();
                if (colSqlType == Types.BLOB)
                {
                    data = resultSet.getBytes(column.getName());
                }
                else
                if (colSqlType == Types.VARCHAR || colSqlType == Types.CLOB)
                {
                    // includes TEXT type, but if TEXT is read with .getObject the
                    // returned type is in h2 case org.h2.jdbc.JdbcClob instead of String
                    data = resultSet.getString(column.getName());
                }
                else
                {
                    data = resultSet.getObject(column.getName());
                }
                if (resultSet.wasNull())
                {
                    data = null;
                }
                objects.put(column.getName(), data);
            }
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }

        Pair<DATA, INIT_MAPS> pair;
        try
        {
            pair = dataLoader.loadImpl(new RawParameters(table, objects), parents);
        }
        catch (InvalidNameException | InvalidIpAddressException | ValueOutOfRangeException exc)
        {
            StringBuilder pk = new StringBuilder("Primary key: ");
            for (Column col : columns)
            {
                if (col.isPk())
                {
                    pk.append(col.getName()).append(" = '").append(objects.get(col.getName())).append("', ");
                }
            }
            pk.setLength(pk.length() - 2);
            throw new LinStorDBRuntimeException(
                String.format(
                    "Database entry of table %s could not be restore.",
                    table.getName()
                ),
                null,
                null,
                null,
                pk.toString(),
                exc
            );
        }
        return pair;
    }

    Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    String generateUpdateStatement(Column colToUpdate)
    {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ")
            .append(colToUpdate.getTable().getName())
            .append(" SET ").append(colToUpdate.getName()).append(" = ? WHERE ");
        for (Column col : colToUpdate.getTable().values())
        {
            if (col.isPk())
            {
                sql.append(col.getName()).append(DELIMITER_EQUALS_AND);
            }
        }
        sql.setLength(sql.length() - DELIMITER_AND.length());

        return sql.toString();
    }

    <DATA> int setPrimaryValues(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        PreparedStatement stmt,
        int startIdxRef,
        DatabaseTable table,
        DATA data
    )
        throws SQLException, DatabaseException, AccessDeniedException
    {
        return setValues(setters, stmt, startIdxRef, table, Column::isPk, data);
    }

    <DATA> int setValues(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        PreparedStatement stmt,
        int startIdxRef,
        DatabaseTable table,
        Predicate<Column> predicate,
        DATA data
    )
        throws SQLException, DatabaseException, AccessDeniedException
    {
        int idx = startIdxRef;
        for (Column col : table.values())
        {
            if (predicate.test(col))
            {
                Object obj = setters.get(col).accept(data);
                if (obj == null)
                {
                    if (col.isNullable())
                    {
                        switch (col.getSqlType())
                        {
                            case Types.BLOB:
                                stmt.setBytes(idx, (byte[]) obj);
                                break;
                            default:
                                stmt.setNull(idx, col.getSqlType());
                                break;
                        }
                    }
                    else
                    {
                        throw new DatabaseException(
                            "Cannot persist null object to not null database column.",
                            null,
                            null,
                            null,
                            "Table: " + table.getName() + ", Column: " + col.getName()
                        );
                    }
                }
                else
                {
                    try
                    {
                        switch (col.getSqlType())
                        {
                            case Types.BLOB:
                                stmt.setBytes(idx, (byte[]) obj);
                                break;
                            case Types.CLOB:
                                stmt.setString(idx, (String) obj);
                                break;
                            default:
                                stmt.setObject(idx, obj, col.getSqlType());
                                break;
                        }
                    }
                    catch (Exception exc)
                    {
                        throw new LinStorDBRuntimeException(
                            "Could not set object '" + obj.toString() + "' of type " + obj.getClass().getSimpleName() +
                                " as SQL type: " + col.getSqlType() + " (" + JDBCType.valueOf(col.getSqlType()) +
                                ") for column " + table.getName() + "." + col.getName(),
                            exc
                        );
                    }
                }
                ++idx;
            }
        }
        return idx;
    }

    @Override
    public String getDbDump() throws DatabaseException
    {
        DbDump dump = SqlDump.getDump(getConnection());
        return dump.serializeHuman();
    }
}
