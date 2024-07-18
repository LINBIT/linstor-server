package com.linbit.linstor.dbdrivers.sql;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportPojoData;
import com.linbit.linstor.core.cfg.CtrlConfig;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
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
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

@Singleton
public class SQLEngine implements DbEngine
{
    private static final String DELIMITER_AND = "AND ";
    private static final String DELIMITER_COMMA = ", ";
    private static final String DELIMITER_EQUALS_AND = " = ? " + DELIMITER_AND;
    private static final String DELIMITER_EQUALS_COMMA = " = ? " + DELIMITER_COMMA;
    private static final String DELIMITER_LIST = ", ";
    private static final String DELIMITER_VALUES = "?" + DELIMITER_LIST;

    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgrSQL> transMgrProvider;
    private final HashMap<DatabaseTable, String> selectAllStatements;
    private final HashMap<DatabaseTable, String> selectSingleStatements;
    private final HashMap<DatabaseTable, String> insertStatements;
    private final HashMap<DatabaseTable, String> updateSingleStatements;
    private final HashMap<DatabaseTable, String> deleteStatements;
    private final HashMap<DatabaseTable, String> truncateStatements;
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

        selectAllStatements = new HashMap<>();
        selectSingleStatements = new HashMap<>();
        insertStatements = new HashMap<>();
        updateSingleStatements = new HashMap<>();
        deleteStatements = new HashMap<>();
        truncateStatements = new HashMap<>();
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
            try (PreparedStatement stmt = getConnection().prepareStatement("BACKUP TO ?"))
            {
                stmt.setString(1, backupPath);
                stmt.execute();
                final ApiCallRcImpl.ApiCallRcEntry rc = ApiCallRcImpl.entryBuilder(
                        ApiConsts.MASK_SUCCESS | ApiConsts.MASK_CRT,
                        "Database backup created: " + backupPath)
                    .putObjRef("backup_location", backupPath)
                    .build();
                apiCallRc.addEntry(rc);
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
        try
        {
            errorReporter.logTrace("Creating %s %s", table.getName(), dataToString.toString(data));

            insertImpl(setters, data, table);

            errorReporter.logTrace("%s created %s", table.getName(), dataToString.toString(data));
        }
        catch (AccessDeniedException exc)
        {
            DatabaseLoader.handleAccessDeniedException(exc);
        }
    }

    private <DATA> void insertImpl(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        DatabaseTable table
    )
        throws DatabaseException, AccessDeniedException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(getInsertStatement(table)))
        {
            setValues(setters, stmt, 1, table, ignored -> true, data);

            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    private String getSelectAllStatement(DatabaseTable table)
    {
        String sql = selectAllStatements.get(table);
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
            selectAllStatements.put(table, sql);
        }
        return sql;
    }

    private String getInsertStatement(DatabaseTable table)
    {
        String sql = insertStatements.get(table);
        if (sql == null)
        {
            List<String> clmNamesList = new ArrayList<>();
            for (Column col : table.values())
            {
                clmNamesList.add(col.getName());
            }

            sql = getInsertStatement(table.getName(), clmNamesList);
            insertStatements.put(table, sql);
        }
        return sql;
    }

    private String getInsertStatement(DbExportPojoData.Table tableRef)
    {
        // no need for cache
        List<String> clmNamesList = new ArrayList<>();
        for (DbExportPojoData.Column col : tableRef.columnDescription)
        {
            clmNamesList.add(col.name);
        }

        return getInsertStatement(tableRef.name, clmNamesList);
    }

    private String getInsertStatement(String tblNameRef, List<String> clmNamesListRef)
    {
        StringBuilder values = new StringBuilder();
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tblNameRef).append(" (");
        for (String col : clmNamesListRef)
        {
            sqlBuilder.append(col).append(DELIMITER_LIST);
            values.append(DELIMITER_VALUES);
        }
        sqlBuilder.setLength(sqlBuilder.length() - DELIMITER_LIST.length());
        values.setLength(values.length() - DELIMITER_LIST.length());

        sqlBuilder.append(") VALUES(").append(values).append(")");

        return sqlBuilder.toString();
    }

    @Override
    public <DATA> void upsert(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        DATA dataRef,
        DatabaseTable tableRef,
        DataToString<DATA> dataToStringRef
    )
        throws DatabaseException, AccessDeniedException
    {
        try (PreparedStatement upsertStmt = getConnection().prepareStatement(getSelectSingleStatement(tableRef)))
        {
            errorReporter.logTrace("Upserting %s %s", tableRef.getName(), dataToStringRef.toString(dataRef));

            setPrimaryValues(settersRef, upsertStmt, 1, tableRef, dataRef);
            ResultSet resultSet = upsertStmt.executeQuery();
            if (resultSet.next())
            {
                errorReporter.logTrace(
                    "Entry exists. Updating %s %s",
                    tableRef.getName(),
                    dataToStringRef.toString(dataRef)
                );
                try (
                    PreparedStatement updateStmt = getConnection().prepareStatement(getUpdateSingleStatement(tableRef)))
                {
                    /*
                     * UPDATE <table> SET $NON-PK1 = ?, ... WHERE $PK1 = ? AND ...
                     */
                    int idx = setValues(settersRef, updateStmt, 1, tableRef, clm -> !clm.isPk(), dataRef);
                    setPrimaryValues(settersRef, updateStmt, idx, tableRef, dataRef);

                    updateStmt.executeUpdate();
                }
            }
            else
            {
                errorReporter.logTrace(
                    "Entry did not exist. Inserting %s %s",
                    tableRef.getName(),
                    dataToStringRef.toString(dataRef)
                );
                insertImpl(settersRef, dataRef, tableRef);
            }
            errorReporter.logTrace("%s upserted %s", tableRef.getName(), dataToStringRef.toString(dataRef));
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

    private String getSelectSingleStatement(DatabaseTable table)
    {
        String sql = selectSingleStatements.get(table);
        if (sql == null)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append(getSelectAllStatement(table));
            sqlBuilder.append(" WHERE ");

            for (Column col : table.values())
            {
                if (col.isPk())
                {
                    sqlBuilder.append(col.getName()).append(DELIMITER_EQUALS_AND);
                }
            }
            sqlBuilder.setLength(sqlBuilder.length() - DELIMITER_AND.length());

            sql = sqlBuilder.toString();
            selectSingleStatements.put(table, sql);
        }
        return sql;
    }

    private String getUpdateSingleStatement(DatabaseTable table)
    {
        String sql = updateSingleStatements.get(table);
        if (sql == null)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("UPDATE ");
            sqlBuilder.append(table.getName());
            sqlBuilder.append(" SET ");
            for (Column col : table.values())
            {
                if (!col.isPk())
                {
                    sqlBuilder.append(col.getName()).append(DELIMITER_EQUALS_COMMA);
                }
            }
            sqlBuilder.setLength(sqlBuilder.length() - DELIMITER_COMMA.length());
            sqlBuilder.append(" WHERE ");
            for (Column col : table.values())
            {
                if (col.isPk())
                {
                    sqlBuilder.append(col.getName()).append(DELIMITER_EQUALS_AND);
                }
            }
            sqlBuilder.setLength(sqlBuilder.length() - DELIMITER_AND.length());

            sql = sqlBuilder.toString();
            updateSingleStatements.put(table, sql);
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
    public void truncate(DatabaseTable table) throws DatabaseException
    {
        try (PreparedStatement stmt = getConnection().prepareStatement(getTruncateStatement(table)))
        {
            errorReporter.logTrace("Truncating table %s", table.getName());

            stmt.executeUpdate();

            errorReporter.logTrace("Table %s truncated", table.getName());
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    private String getTruncateStatement(DatabaseTable table)
    {
        String sql = truncateStatements.get(table);
        if (sql == null)
        {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("DELETE FROM ").append(table.getName());
            sql = sqlBuilder.toString();
            truncateStatements.put(table, sql);
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
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToStringRef,
        DataToString<INPUT_TYPE> inputToStringRef
    )
    {
        return new SQLSingleColumnDriver<>(
            this,
            errorReporter,
            setters,
            colRef,
            typeMapperRef,
            dataToStringRef,
            dataValueToStringRef,
            inputToStringRef
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
    public <DATA, KEY, VALUE> MapDatabaseDriver<DATA, KEY, VALUE> generateMapToJsonStringArrayDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        DataToString<DATA> dataToStringRef
    )
    {
        return new SQLMapToJsonDriver<>(this, errorReporter, setters, colRef, dataToStringRef);
    }

    @Override
    public <DATA extends Comparable<? super DATA>, INIT_MAPS, LOAD_ALL> Map<DATA, INIT_MAPS> loadAll(
        DatabaseTable table,
        LOAD_ALL parentsRef,
        DataLoader<DATA, INIT_MAPS, LOAD_ALL> dataLoaderRef
    )
        throws DatabaseException, AccessDeniedException, MdException
    {
        Map<DATA, INIT_MAPS> loadedObjectsMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(getSelectAllStatement(table)))
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
        RawParameters rawParams = buildRawParams(table, resultSet, columns, objects);

        Pair<DATA, INIT_MAPS> pair;
        try
        {
            pair = dataLoader.loadImpl(rawParams, parents);
        }
        catch (LinStorDBRuntimeException exc)
        {
            throw exc;
        }
        catch (InvalidNameException | InvalidIpAddressException | ValueOutOfRangeException | RuntimeException |
            AccessDeniedException | ValueInUseException | ExhaustedPoolException exc)
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
                    "Database entry of table %s could not be restored.",
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

    private RawParameters buildRawParams(
        DatabaseTable table,
        ResultSet resultSet,
        Column[] columns,
        Map<String, Object> objects
    )
        throws DatabaseException
    {
        try
        {
            for (Column column : columns)
            {
                Object data;
                int colSqlType = column.getSqlType();
                switch (colSqlType)
                {
                    case Types.BLOB:
                        data = resultSet.getBytes(column.getName());
                        break;
                    case Types.VARCHAR: // fall-through
                    case Types.CLOB:
                        // includes TEXT type, but if TEXT is read with .getObject the
                        // returned type is in h2 case org.h2.jdbc.JdbcClob instead of String
                        data = resultSet.getString(column.getName());
                        break;
                    case Types.SMALLINT:
                        // some jdbc drivers (like mariadb / postgresql) would return an Integer here, which cannot be
                        // casted later on to Short
                        data = resultSet.getShort(column.getName());
                        break;
                    case Types.TIMESTAMP:
                        Timestamp timestamp = resultSet.getTimestamp(column.getName());
                        data = timestamp != null ? timestamp.getTime() : null;
                        break;
                    default:
                        data = resultSet.getObject(column.getName());
                        break;
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
        return new RawParameters(table, objects, DatabaseType.SQL);
    }

    @Override
    public List<RawParameters> export(DatabaseTable tableRef) throws DatabaseException
    {
        List<RawParameters> ret = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(getSelectAllStatement(tableRef)))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    ret.add(buildRawParams(
                        tableRef,
                        resultSet,
                        tableRef.values(),
                        new TreeMap<>()
                    ));
                }
            }
        }
        catch (SQLException exc)
        {
            throw new DatabaseException(exc);
        }
        return ret;
    }

    @Override
    public void truncateAllData(List<DbExportPojoData.Table> orderedTablesListRef) throws DatabaseException
    {
        for (DbExportPojoData.Table tbl : orderedTablesListRef)
        {
            try (PreparedStatement stmt = getConnection().prepareStatement("DELETE FROM " + tbl.name))
            {
                stmt.executeUpdate();
            }
            catch (SQLException sqlExc)
            {
                throw new DatabaseException(
                    "Could not delete all default values before importing data to table: " + tbl.name,
                    sqlExc
                );
            }
        }
    }

    @Override
    public void importData(DbExportPojoData.Table tableRef) throws DatabaseException
    {
        LinstorSpec<?, ?> currentSpec = null;

        try (PreparedStatement stmt = getConnection().prepareStatement(getInsertStatement(tableRef)))
        {
            for (LinstorSpec<?, ?> linstorSpec : tableRef.data)
            {
                currentSpec = linstorSpec;
                setValuesFromSpec(stmt, tableRef, linstorSpec);
                stmt.executeUpdate();
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(
                "Table: " + tableRef.name + ", entry: " + (currentSpec == null ?
                    "<null>" :
                    currentSpec.getLinstorKey()),
                sqlExc
            );
        }
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
                setSqlParam(
                    stmt,
                    idx,
                    col.isNullable(),
                    col.getSqlType(),
                    obj,
                    table.getName(),
                    col.getName()
                );
                ++idx;
            }
        }
        return idx;
    }

    private void setSqlParam(
        PreparedStatement stmtRef,
        int idxRef,
        boolean nullableRef,
        int sqlTypeRef,
        Object objRef,
        String tblNameRef,
        String clmNameRef
    )
        throws SQLException, DatabaseException
    {
        if (objRef == null)
        {
            if (nullableRef)
            {
                switch (sqlTypeRef)
                {
                    case Types.BLOB:
                        stmtRef.setBytes(idxRef, (byte[]) objRef);
                        break;
                    default:
                        stmtRef.setNull(idxRef, sqlTypeRef);
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
                    "Table: " + tblNameRef + ", Column: " + clmNameRef
                );
            }
        }
        else
        {
            try
            {
                switch (sqlTypeRef)
                {
                    case Types.BLOB:
                        stmtRef.setBytes(idxRef, (byte[]) objRef);
                        break;
                    case Types.CLOB:
                        stmtRef.setString(idxRef, (String) objRef);
                        break;
                    case Types.TIMESTAMP:
                        Timestamp timestamp;
                        if (objRef instanceof Timestamp)
                        {
                            timestamp = (Timestamp) objRef;
                        }
                        else
                        {
                            timestamp = new Timestamp((Long) objRef);
                        }
                        stmtRef.setTimestamp(idxRef, timestamp);
                        break;
                    case Types.DATE:
                        long dateTimestamp;
                        if (objRef instanceof java.util.Date)
                        {
                            dateTimestamp = ((java.util.Date) objRef).getTime();
                        }
                        else
                        {
                            dateTimestamp = (Long) objRef;
                        }
                        stmtRef.setDate(idxRef, new java.sql.Date(dateTimestamp));
                        break;
                    default:
                        stmtRef.setObject(idxRef, objRef, sqlTypeRef);
                        break;
                }
            }
            catch (Exception exc)
            {
                throw new LinStorDBRuntimeException(
                    "Could not set object '" + objRef.toString() + "' of type " + objRef.getClass().getSimpleName() +
                        " as SQL type: " + sqlTypeRef + " (" + JDBCType.valueOf(sqlTypeRef) +
                        ") for column " + tblNameRef + "." + clmNameRef,
                    exc
                );
            }
        }
    }

    private void setValuesFromSpec(
        PreparedStatement stmtRef,
        DbExportPojoData.Table tableRef,
        LinstorSpec<?, ?> linstorSpecRef
    )
        throws DatabaseException, SQLException
    {
        int idx = 1;
        for (DbExportPojoData.Column col : tableRef.columnDescription)
        {
            setSqlParam(
                stmtRef,
                idx,
                col.isNullable,
                col.sqlType,
                linstorSpecRef.getByColumn(col.name),
                tableRef.name,
                col.name
            );
            idx++;
        }
    }

    @Override
    public String getDbDump() throws DatabaseException
    {
        DbDump dump = SqlDump.getDump(getConnection());
        return dump.serializeHuman();
    }
}
