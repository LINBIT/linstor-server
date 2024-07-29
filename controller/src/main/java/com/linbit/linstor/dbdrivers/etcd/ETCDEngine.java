package com.linbit.linstor.dbdrivers.etcd;

import com.linbit.ExhaustedPoolException;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.db.DbExportPojoData;
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
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.EtcdTransaction;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;
import com.linbit.utils.Base64;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.inject.Provider;

@Singleton
public class ETCDEngine extends BaseEtcdDriver implements DbEngine
{
    private final ErrorReporter errorReporter;
    private final Set<DatabaseTable> disabledRecursiveDeleteDbTables;

    @Inject
    public ETCDEngine(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        errorReporter = errorReporterRef;
        disabledRecursiveDeleteDbTables = new HashSet<>();
    }

    @Override
    public DatabaseType getType()
    {
        return DatabaseType.ETCD;
    }

    public void disableRecursiveDelete(DatabaseTable dbTableRef)
    {
        disabledRecursiveDeleteDbTables.add(dbTableRef);
    }

    @Override
    public ApiCallRc backupDb(String backupPath) throws DatabaseException
    {
        return ApiCallRcImpl.singleApiCallRc(
            ApiConsts.FAIL_UNKNOWN_ERROR, "Only h2 database is currently supported for online backup.");
    }

    @Override
    public <DATA> void create(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        DatabaseTable table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException
    {
        EtcdTransaction tx = transMgrProvider.get().getTransaction();
        for (Column col : table.values())
        {
            if (!col.isPk())
            {
                String key = getColumnKey(setters, col, data);

                Object obj = setters.get(col).accept(data);
                if (obj != null)
                {
                    tx.put(key, Objects.toString(obj));
                }
                else
                {
                    tx.put(key, DUMMY_NULL_VALUE);
                }
            }
        }
        // sync will be called within transMgr.commit()
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
        // ETCD does not distinguish between update or insert / create.
        create(settersRef, dataRef, tableRef, dataToStringRef);
    }

    @Override
    public <DATA> void delete(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        DatabaseTable table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException
    {
        EtcdTransaction tx = transMgrProvider.get().getTransaction();

        String[] pks = getPrimaryKeys(table, data, setters);
        for (Column col : table.values())
        {
            // do not use ranged delete since quite a few database tables can be effected by some "recreate" event (i.e.
            // delete and create the same object in the same transaction).
            // that causes "DELETE /key/*" as well as "PUT /key/column=..." events, which cannot be deduplicated
            // properly, causing an ETCD exception
            tx.delete(EtcdUtils.buildKey(col, pks));
        }
        // sync will be called within transMgr.commit()
    }

    @Override
    public void truncate(DatabaseTable tableRef) throws DatabaseException
    {
        EtcdTransaction tx = transMgrProvider.get().getTransaction();

        // do not use ranged delete since quite a few database tables can be effected by some "recreate" event (i.e.
        // delete and create the same object in the same transaction).
        // that causes "DELETE /key/*" as well as "PUT /key/column=..." events, which cannot be deduplicated
        // properly, causing an ETCD exception
        Map<String, String> dataMap = new TreeMap<>(namespace(tableRef).get(true));
        for (String etcdKey : dataMap.keySet())
        {
            tx.delete(etcdKey);
        }
    }

    @Override
    public <DATA extends Comparable<? super DATA>, INIT_MAPS, LOAD_ALL> Map<DATA, INIT_MAPS> loadAll(
        DatabaseTable table,
        LOAD_ALL parents,
        DataLoader<DATA, INIT_MAPS, LOAD_ALL> dataLoader
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException, InvalidIpAddressException,
        ValueOutOfRangeException, MdException, ValueInUseException, ExhaustedPoolException
    {
        Map<DATA, INIT_MAPS> loadedObjectsMap = new TreeMap<>();
        final Column[] columns = table.values();

        Map<String, String> dataMap = new TreeMap<>(namespace(table).get(true));

        Set<String> composedPkList = EtcdUtils.getComposedPkList(dataMap);
        for (String composedPk : composedPkList)
        {
            Map<String, Object> rawObjects = new TreeMap<>();
            RawParameters rawParameters = buildRawParams(table, dataMap, composedPk, rawObjects);
            Pair<DATA, INIT_MAPS> pair;
            try
            {
                pair = dataLoader.loadImpl(
                    rawParameters,
                    parents
                );
            }
            catch (LinStorDBRuntimeException exc)
            {
                throw exc;
            }
            catch (InvalidNameException | InvalidIpAddressException | ValueOutOfRangeException | RuntimeException exc)
            {
                StringBuilder pk = new StringBuilder("Primary key: ");
                for (Column col : columns)
                {
                    if (col.isPk())
                    {
                        pk.append(col.getName()).append(" = '").append(rawObjects.get(col.getName())).append("', ");
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
            // pair might be null when loading objects sharing the same table.
            // For example SnapshotDbDriver will return null when finding a Resource entry
            // and vice versa.
            if (pair != null)
            {
                loadedObjectsMap.put(pair.objA, pair.objB);
            }
        }

        return loadedObjectsMap;
    }

    private RawParameters buildRawParams(
        DatabaseTable table,
        Map<String, String> dataMap,
        @Nullable String composedPk,
        Map<String, Object> rawObjects
    )
    {
        String[] pks = EtcdUtils.splitPks(composedPk, false);

        int pkIdx = 0;

        for (Column col : table.values())
        {
            if (col.isPk())
            {
                rawObjects.put(col.getName(), pks[pkIdx++]);
            }
            else
            {
                String colKey = EtcdUtils.buildKey(col, pks);
                String colData = dataMap.get(colKey);
                if (colData == null && !col.isNullable())
                {
                    throw new LinStorDBRuntimeException("Column was unexpectedly null. " + colKey);
                }
                if (DUMMY_NULL_VALUE.equals(colData) || colData == null)
                {
                    rawObjects.put(col.getName(), null);
                }
                else
                {
                    rawObjects.put(col.getName(), colData);
                }
            }
        }
        return new RawParameters(table, rawObjects, DatabaseType.ETCD);
    }

    private <DATA> String getPk(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DatabaseTable table,
        DATA data
    )
        throws AccessDeniedException
    {
        StringBuilder sb = new StringBuilder();
        sb.append(EtcdUtils.linstorPrefix).append(table.getName()).append(EtcdUtils.PATH_DELIMITER);

        for (Column col : table.values())
        {
            if (col.isPk())
            {
                sb.append(Objects.toString(setters.get(col).accept(data))).append(EtcdUtils.PK_DELIMITER);
            }
        }
        sb.setLength(sb.length() - EtcdUtils.PK_DELIMITER.length()); // cut last PK_DELIMITER
        sb.append(EtcdUtils.PATH_DELIMITER);
        return sb.toString();
    }

    private <DATA> String getColumnKey(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column col,
        DATA data
    )
        throws AccessDeniedException
    {
        return getPk(setters, col.getTable(), data) + col.getName();
    }

    @Override
    public <DATA, FLAG extends Enum<FLAG> & Flags> StateFlagsPersistence<DATA> generateFlagsDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column flagColumn,
        Class<FLAG> flagsClass,
        DataToString<DATA> dataToString
    )
    {
        return (data, oldFlagBits, newFlagBits) ->
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(flagsClass, oldFlagBits),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(flagsClass, newFlagBits),
                    ", "
                );
                String inlineId = dataToString.toString(data);

                errorReporter
                    .logTrace(
                        "Updating %s's flags from [%s] to [%s] %s",
                        flagColumn.getTable().getName(),
                        fromFlags,
                        toFlags,
                        inlineId
                    );
                namespace(flagColumn.getTable(), getPrimaryKeys(flagColumn.getTable(), data, setters))
                    .put(flagColumn, Long.toString(newFlagBits));
            }
            catch (AccessDeniedException exc)
            {
                DatabaseLoader.handleAccessDeniedException(exc);

            }
        };
    }

    @Override
    public <DATA, LIST_TYPE> CollectionDatabaseDriver<DATA, LIST_TYPE> generateCollectionToJsonStringArrayDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column colRef,
        DataToString<DATA> dataToStringRef
    )
    {
        return new ETCDListToJsonArrayDriver<>(
            errorReporter,
            settersRef,
            colRef,
            dataToStringRef,
            transMgrProvider
        );
    }

    @Override
    public <DATA, KEY, VALUE> MapDatabaseDriver<DATA, KEY, VALUE> generateMapToJsonStringArrayDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column colRef,
        DataToString<DATA> dataToStringRef
    )
    {
        return new ETCDMapToJsonDriver<>(
            errorReporter,
            settersRef,
            colRef,
            dataToStringRef,
            transMgrProvider
        );
    }

    private <DATA> String[] getPrimaryKeys(
        DatabaseTable table,
        DATA data,
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters
    )
        throws AccessDeniedException
    {
        List<String> pkList = new ArrayList<>();
        for (Column col : table.values())
        {
            if (col.isPk())
            {
                pkList.add(Objects.toString(setters.get(col).accept(data)));
            }
        }

        String[] pks = new String[pkList.size()];
        pkList.toArray(pks);
        return pks;
    }

    @Override
    public <DATA, INPUT_TYPE, DB_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateSingleColumnDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column col,
        Function<INPUT_TYPE, DB_TYPE> typeMapper,
        DataToString<DATA> dataToString,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToString,
        DataToString<INPUT_TYPE> ignoredInputToString
    )
    {
        return (data, oldElement) ->
        {
            try
            {
                errorReporter.logTrace(
                    "Updating %s's %s from [%s] to [%s] %s",
                    col.getTable().getName(),
                    col.getName(),
                    typeMapper.apply(oldElement),
                    dataValueToString.accept(data),
                    dataToString.toString(data)
                );

                Object newValue = setters.get(col).accept(data);

                if (newValue != null)
                {
                    namespace(col.getTable(), getPrimaryKeys(col.getTable(), data, setters))
                        .put(col, Objects.toString(newValue));
                }
                else
                {
                    namespace(EtcdUtils.buildKey(col, getPrimaryKeys(col.getTable(), data, setters)))
                        .delete();
                }
            }
            catch (AccessDeniedException exc)
            {
                DatabaseLoader.handleAccessDeniedException(exc);

            }
        };
    }

    @Override
    public List<RawParameters> export(DatabaseTable tableRef) throws DatabaseException
    {
        List<RawParameters> ret = new ArrayList<>();
        Map<String, String> dataMap = new TreeMap<>(namespace(tableRef).get(true));

        // no data -> nothing to add to the returned list
        if (!dataMap.isEmpty())
        {
            // check for corner case where we might not have a primary key, i.e. "tables" with only one entry
            boolean tableHasPrimaryKey = false;
            for (Column clm : tableRef.values())
            {
                if (clm.isPk())
                {
                    tableHasPrimaryKey = true;
                    break;
                }
            }

            if (tableHasPrimaryKey)
            {
                Set<String> composedPkList = EtcdUtils.getComposedPkList(dataMap);
                if (!composedPkList.isEmpty())
                {
                    for (String composedPk : composedPkList)
                    {
                        Map<String, Object> rawObjects = new TreeMap<>();
                        ret.add(buildRawParams(tableRef, dataMap, composedPk, rawObjects));
                    }
                }
            }
            else
            {
                Map<String, Object> rawObjects = new TreeMap<>();
                ret.add(buildRawParams(tableRef, dataMap, null, rawObjects));
            }
        }

        return ret;
    }

    @Override
    public void truncateAllData(List<DbExportPojoData.Table> tablesToTruncateRef) throws DatabaseException
    {
        EtcdTransaction tx = transMgrProvider.get().getTransaction();
        for (DbExportPojoData.Table tbl : tablesToTruncateRef)
        {
            TreeMap<String, String> allDataFromTbl = tx.get(EtcdUtils.buildKey(tbl.name, false), true);

            // since we are using the same transaction for truncate and import, we should NOT use recursive = true here
            // so that the deduplicator can do its job
            for (String key : allDataFromTbl.keySet())
            {
                tx.delete(key, false);
            }
        }
    }

    @Override
    public void importData(DbExportPojoData.Table tableRef) throws DatabaseException
    {
        List<DbExportPojoData.Column> pkCols = new ArrayList<>();
        for (DbExportPojoData.Column column : tableRef.columnDescription)
        {
            if (column.isPrimaryKey)
            {
                pkCols.add(column);
            }
        }

        EtcdTransaction tx = transMgrProvider.get().getTransaction();
        for (LinstorSpec<?, ?> linstorSpec : tableRef.data)
        {
            String[] pks = new String[pkCols.size()];
            int pkIdx = 0;
            for (DbExportPojoData.Column column : pkCols)
            {
                Object value = linstorSpec.getByColumn(column.name);
                if (value instanceof Date)
                {
                    value = ((Date) value).getTime(); // ETCD should store all dates as long
                }
                pks[pkIdx++] = Objects.toString(value);
            }

            boolean allColumnsPrimary = true;
            for (DbExportPojoData.Column column : tableRef.columnDescription)
            {
                if (!column.isPrimaryKey)
                {
                    allColumnsPrimary = false;
                    break;
                }
            }

            for (DbExportPojoData.Column column : tableRef.columnDescription)
            {
                if (!column.isPrimaryKey || allColumnsPrimary)
                {
                    Object value = linstorSpec.getByColumn(column.name);
                    if (value == null)
                    {
                        value = DUMMY_NULL_VALUE;
                    }
                    else if (column.sqlType == Types.BLOB)
                    {
                        value = Base64.encode((byte[]) value);
                    }
                    else if (column.sqlType == Types.DATE)
                    {
                        value = ((Date) value).getTime();
                    }

                    tx.put(
                        EtcdUtils.buildKeyStr(tableRef.name, column.name, pks),
                        Objects.toString(value)
                    );
                }
            }
        }
    }


    @Override
    public String getDbDump()
    {
        EtcdTransaction tx = transMgrProvider.get().getTransaction();
        TreeMap<String, String> dump = tx.get(EtcdUtils.linstorPrefix, true);
        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> ent : dump.entrySet())
        {
            sb.append(ent.getKey() + "\t" + ent.getValue() + "\n");
        }
        return sb.toString();
    }
}
