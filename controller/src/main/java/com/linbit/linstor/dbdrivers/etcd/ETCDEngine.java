package com.linbit.linstor.dbdrivers.etcd;

import static com.ibm.etcd.client.KeyUtils.bs;

import com.linbit.CollectionDatabaseDriver;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.dbdrivers.AbsDatabaseDriver.RawParameters;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Table;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.TransactionMgrETCD;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;
import com.linbit.utils.StringUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.inject.Provider;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.DeleteRangeRequest;
import com.ibm.etcd.api.DeleteRangeRequestOrBuilder;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.client.KeyUtils;
import com.ibm.etcd.client.kv.KvClient.FluentTxnOps;

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
        Table table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException
    {
        FluentTxnOps<?> transaction = transMgrProvider.get().getTransaction();
        for (Column col : table.values())
        {
            if (!col.isPk())
            {
                String key = getColumnKey(setters, col, data);

                Object obj = setters.get(col).accept(data);
                if (obj != null)
                {
                    PutRequest putRequest = PutRequest.newBuilder()
                        .setKey(bs(key))
                        .setValue(bs(Objects.toString(obj)))
                        .build();

                    transaction.put(putRequest);
                }
            }
        }
        // sync will be called within transMgr.commit()
    }

    @Override
    public <DATA> void delete(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        Table table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException
    {
        FluentTxnOps<?> transaction = transMgrProvider.get().getTransaction();

        String key = getPk(setters, table, data);

        ByteString bsKey = bs(key);
        DeleteRangeRequestOrBuilder deleteRequest = DeleteRangeRequest.newBuilder()
            .setKey(bsKey)
            .setRangeEnd(KeyUtils.plusOne(bsKey))
            .build();

        transaction.delete(deleteRequest);
        // sync will be called within transMgr.commit()
    }

    @Override
    public <DATA, INIT_MAPS, LOAD_ALL> Map<DATA, INIT_MAPS> loadAll(
        Table table,
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
            Object[] rawObjects = new Object[columns.length];
            String[] pks = EtcdUtils.splitPks(composedPk, false);

            int pkIdx = 0;

            for (Column col : columns)
            {
                if (col.isPk())
                {
                    rawObjects[col.getIndex()] = pks[pkIdx++];
                }
                else
                {
                    String colKey = EtcdUtils.buildKey(col, pks);
                    String colData = dataMap.get(colKey);
                    if (colData == null && !col.isNullable())
                    {
                        throw new LinStorDBRuntimeException("Column was unexpectedly null. " + colKey);
                    }
                    rawObjects[col.getIndex()] = colData;
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
        Table table,
        DATA data
    )
        throws AccessDeniedException
    {
        StringBuilder sb = new StringBuilder();
        sb.append(EtcdUtils.LINSTOR_PREFIX).append(table.getName()).append(EtcdUtils.PATH_DELIMITER);

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
        return new ETCDListToJsonArrayDriver<DATA, LIST_TYPE>(
            errorReporter,
            settersRef,
            colRef,
            dataToStringRef,
            transMgrProvider
        );
    }

    private <DATA> String[] getPrimaryKeys(
        Table table,
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
                    Objects.toString(colValue),
                    dataToString.toString(data)
                );
                if (colValue != null)
                {
                    namespace(col.getTable(), getPrimaryKeys(col.getTable(), data, setters))
                        .put(col, Objects.toString(colValue));
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
}
