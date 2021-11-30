package com.linbit.linstor.dbdrivers.etcd;

import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver.RawParameters;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.EtcdTransaction;
import com.linbit.linstor.transaction.manager.TransactionMgrETCD;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
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

    @Inject
    public ETCDEngine(
        ErrorReporter errorReporterRef,
        Provider<TransactionMgrETCD> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);
        errorReporter = errorReporterRef;
    }

    @Override
    public DatabaseType getType()
    {
        return DatabaseType.ETCD;
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
            }
        }
        // sync will be called within transMgr.commit()
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

        String key = getPk(setters, table, data);

        tx.delete(key, true);
        // sync will be called within transMgr.commit()
    }

    @Override
    public <DATA, INIT_MAPS, LOAD_ALL> Map<DATA, INIT_MAPS> loadAll(
        DatabaseTable table,
        LOAD_ALL parents,
        DataLoader<DATA, INIT_MAPS, LOAD_ALL> dataLoader
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException, InvalidIpAddressException,
        ValueOutOfRangeException, MdException
    {
        Map<DATA, INIT_MAPS> loadedObjectsMap = new TreeMap<>();
        final Column[] columns = table.values();

        Map<String, String> dataMap = new TreeMap<>(namespace(table).get(true));
        Set<String> composedPkList = EtcdUtils.getComposedPkList(dataMap);
        for (String composedPk : composedPkList)
        {
            Map<String, Object> rawObjects = new TreeMap<>();
            String[] pks = EtcdUtils.splitPks(composedPk, false);

            int pkIdx = 0;

            for (Column col : columns)
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
                    rawObjects.put(col.getName(), colData);
                }
            }
            Pair<DATA, INIT_MAPS> pair = dataLoader.loadImpl(new RawParameters(table, rawObjects), parents);
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
        return (data, flags) ->
        {
            try
            {
                String fromFlags = StringUtils.join(
                    FlagsHelper.toStringList(flagsClass, (long) setters.get(flagColumn).accept(data)),
                    ", "
                );
                String toFlags = StringUtils.join(
                    FlagsHelper.toStringList(flagsClass, flags),
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
                    .put(flagColumn, Long.toString(flags));
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
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToString
    )
    {
        return (data, colValue) ->
        {
            try
            {
                errorReporter.logTrace(
                    "Updating %s's %s from [%s] to [%s] %s",
                    col.getTable().getName(),
                    col.getName(),
                    dataValueToString.accept(data),
                    typeMapper.apply(colValue),
                    dataToString.toString(data)
                );
                if (colValue != null)
                {
                    namespace(col.getTable(), getPrimaryKeys(col.getTable(), data, setters))
                        .put(col, Objects.toString(typeMapper.apply(colValue)));
                }
                else
                {
                    namespace(EtcdUtils.buildKey(col, getPrimaryKeys(col.getTable(), data, setters)))
                        .delete(false);
                }
            }
            catch (AccessDeniedException exc)
            {
                DatabaseLoader.handleAccessDeniedException(exc);

            }
        };
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
