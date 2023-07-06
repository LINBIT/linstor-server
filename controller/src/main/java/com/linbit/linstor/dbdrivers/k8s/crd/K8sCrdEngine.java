package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables;
import com.linbit.linstor.dbdrivers.RawParameters;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.K8sCrdTransaction;
import com.linbit.linstor.transaction.manager.TransactionMgrK8sCrd;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;

@Singleton
public class K8sCrdEngine implements DbEngine
{
    private final ObjectMapper objectMapper;
    private final ErrorReporter errorReporter;
    private final Provider<TransactionMgrK8sCrd> transMgrProvider;

    @Inject
    public K8sCrdEngine(ErrorReporter errorReporterRef, Provider<TransactionMgrK8sCrd> transMgrProviderRef)
    {
        errorReporter = errorReporterRef;
        transMgrProvider = transMgrProviderRef;
        objectMapper = new ObjectMapper();
    }

    @Override
    public DatabaseType getType()
    {
        return DatabaseType.K8S_CRD;
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
        DataToString<DATA> dataIdToString
    )
        throws DatabaseException, AccessDeniedException
    {
        try
        {
            K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
            LinstorCrd<?> crd = GenCrdCurrent.dataToCrd(table, setters, data);
            errorReporter.logTrace(
                "Creating %s %s: %n%s",
                table.getName(),
                dataIdToString.toString(data),
                objectMapper.writeValueAsString(crd)
            );
            tx.create(table, crd);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (JsonProcessingException exc)
        {
            throw new DatabaseException(exc);
        }
    }

    private <DATA> void update(
        DatabaseTable table,
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        DataToString<DATA> dataIdToString
    )
        throws DatabaseException
    {
        try
        {
            K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
            LinstorCrd<?> crd = GenCrdCurrent.dataToCrd(table, setters, data);
            errorReporter.logTrace(
                "Updating %s %s: %n%s",
                table.getName(),
                dataIdToString.toString(data),
                objectMapper.writeValueAsString(crd)
            );
            tx.replace(table, crd);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            throw new ImplementationError(accDeniedExc);
        }
        catch (JsonProcessingException exc)
        {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public <DATA> void delete(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA data,
        DatabaseTable table,
        DataToString<DATA> dataIdToString
    )
        throws DatabaseException, AccessDeniedException
    {
        try
        {
            K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
            LinstorCrd<?> crd = GenCrdCurrent.dataToCrd(table, setters, data);
            errorReporter.logTrace(
                "Deleting %s %s: %n%s",
                table.getName(),
                dataIdToString.toString(data),
                objectMapper.writeValueAsString(crd)
            );
            tx.delete(table, crd);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (JsonProcessingException exc)
        {
            throw new DatabaseException(exc);
        }
    }

    @Override
    public <DATA, INIT_MAPS, LOAD_ALL> Map<DATA, INIT_MAPS> loadAll(
        DatabaseTable table,
        LOAD_ALL parents,
        DataLoader<DATA, INIT_MAPS, LOAD_ALL> dataLoader
    )
        throws DatabaseException, AccessDeniedException, InvalidNameException, InvalidIpAddressException,
        ValueOutOfRangeException, MdException, ValueInUseException, ExhaustedPoolException
    {
        Map<DATA, INIT_MAPS> loadedObjectsMap = new TreeMap<>();

        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        for (LinstorSpec linstorSpec : tx.getSpec(table).values())
        {
            Pair<DATA, INIT_MAPS> pair;
            try
            {
                pair = dataLoader.loadImpl(
                    new RawParameters(table, linstorSpec.asRawParameters(), DatabaseType.K8S_CRD),
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
                Map<String, Object> objects = linstorSpec.asRawParameters();
                for (Column col : table.values())
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

    @Override
    public <DATA, FLAG extends Enum<FLAG> & Flags> StateFlagsPersistence<DATA> generateFlagsDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column colRef,
        Class<FLAG> ignoredFlagsClass,
        DataToString<DATA> dataIdToString
    )
    {
        final DatabaseTable table = colRef.getTable();
        // k8s cannot update single "columns" just the whole object. Map all drivers to a simple object-update
        return (data, ignored1, ignored2) -> update(table, settersRef, data, dataIdToString);
    }

    @Override
    public <DATA, INPUT_TYPE, DB_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateSingleColumnDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        Function<INPUT_TYPE, DB_TYPE> ignoredTypeMapper,
        DataToString<DATA> dataIdToString,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToStringRef,
        DataToString<INPUT_TYPE> ignoredInputToString
    )
    {
        final DatabaseTable table = colRef.getTable();
        // k8s cannot update single "columns" just the whole object. Map all drivers to a simple object-update
        return (data, ignored) -> update(table, setters, data, dataIdToString);
    }

    @Override
    public <DATA, LIST_TYPE> CollectionDatabaseDriver<DATA, LIST_TYPE> generateCollectionToJsonStringArrayDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column colRef,
        DataToString<DATA> dataIdToString
    )
    {
        final DatabaseTable table = colRef.getTable();
        // k8s cannot update single "columns" just the whole object. Map all drivers to a simple object-update
        return new CollectionDatabaseDriver<DATA, LIST_TYPE>()
        {
            @Override
            public void insert(DATA parent, LIST_TYPE ignored, Collection<LIST_TYPE> ignoredBackingCollection)
                throws DatabaseException
            {
                update(table, settersRef, parent, dataIdToString);
            }

            @Override
            public void remove(DATA parent, LIST_TYPE ignoredType, Collection<LIST_TYPE> ignoredBackingCollection)
                throws DatabaseException
            {
                update(table, settersRef, parent, dataIdToString);
            }
        };
    }

    @Override
    public List<RawParameters> export(DatabaseTable tableRef) throws DatabaseException
    {
        List<RawParameters> ret = new ArrayList<>();
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        for (LinstorSpec linstorSpec : tx.getSpec(tableRef).values())
        {
            ret.add(new RawParameters(tableRef, linstorSpec.asRawParameters(), DatabaseType.K8S_CRD));
        }

        return ret;
    }

    @Override
    public void importData(DatabaseTable dbTableRef, List<LinstorSpec> dataListRef) throws DatabaseException
    {
        throw new ImplementationError("not implemented");
    }

    @Override
    public String getDbDump() throws DatabaseException
    {
        K8sCrdTransaction tx = transMgrProvider.get().getTransaction();
        Map<String, Collection<LinstorSpec>> dump = new TreeMap<>();
        for (DatabaseTable table : GeneratedDatabaseTables.ALL_TABLES)
        {
            dump.put(table.getName(), tx.getSpec(table).values());
        }
        try
        {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dump);
        }
        catch (JsonProcessingException exc)
        {
            throw new DatabaseException(exc);
        }
    }
}
