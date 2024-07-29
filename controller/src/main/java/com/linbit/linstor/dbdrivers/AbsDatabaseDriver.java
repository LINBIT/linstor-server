package com.linbit.linstor.dbdrivers;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine.DataToString;
import com.linbit.linstor.dbdrivers.interfaces.GenericDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionFactory;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbsDatabaseDriver<DATA extends Comparable<? super DATA>, INIT_MAPS, LOAD_ALL>
    implements GenericDatabaseDriver<DATA>, ControllerDatabaseDriver<DATA, INIT_MAPS, LOAD_ALL>
{
    protected static final String MSG_DO_NOT_LOG = "do not log";

    protected static final ObjectMapper OBJ_MAPPER = new ObjectMapper();
    protected static final TypeReference<List<String>> TYPE_REF_STRING_LIST = new TypeReference<>()
    {
    };
    protected static final TypeReference<Map<String, Integer>> TYPE_REF_STRING_INTEGER_MAP =
        new TypeReference<>()
    {
    };

    protected final AccessContext dbCtx;

    protected final ErrorReporter errorReporter;
    /**
     * The database table to read from and write to.
     * Can be null as some linstor objects (especially layer-related) are skipping "inherited" tables. That means they
     * do need a (rscData-) driver without having a dedicated table to work with, but are still needed in order to
     * correctly load vlmData entries from existing database tables
     */
    protected final @Nullable DatabaseTable table;
    private final DbEngine dbEngine;
    private final @Nullable ObjectProtectionFactory objProtFactory;

    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;

    protected AbsDatabaseDriver(
        @SystemContext AccessContext dbCtxRef,
        ErrorReporter errorReporterRef,
        @Nullable DatabaseTable tableRef,
        DbEngine dbEngineRef,
        @Nullable ObjectProtectionFactory objProtFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        table = tableRef;
        dbEngine = dbEngineRef;
        dbCtx = dbCtxRef;
        objProtFactory = objProtFactoryRef;

        setters = new HashMap<>();
    }

    @Override
    public @Nullable DatabaseTable getDbTable()
    {
        return table;
    }

    @Override
    public List<LinstorSpec<?, ?>> export() throws DatabaseException
    {
        List<RawParameters> export = dbEngine.export(table);

        List<LinstorSpec<?, ?>> specList = new ArrayList<>();
        for (RawParameters rawParam : export)
        {
            LinstorSpec<?, ?> spec = GenCrdCurrent.rawParamToSpec(table, rawParam);
            specList.add(spec);
        }

        return specList;
    }

    @Override
    public void create(DATA dataRef) throws DatabaseException
    {
        if (table != null)
        {
            try
            {
                dbEngine.create(setters, dataRef, table, this::getId);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError("Database driver does not have enough privileges");
            }
        }
    }

    @Override
    public void upsert(DATA dataRef) throws DatabaseException
    {
        if (table != null)
        {
            try
            {
                dbEngine.upsert(setters, dataRef, table, this::getId);
            }
            catch (AccessDeniedException exc)
            {
                throw new ImplementationError("Database driver does not have enough privileges");
            }
        }
    }

    @Override
    public void delete(DATA dataRef) throws DatabaseException
    {
        try
        {
            // some drivers simply do not have a table (like LayerStorageRscDbDriver)
            if (table != null)
            {
                dbEngine.delete(setters, dataRef, table, this::getId);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("Database driver does not have enough privileges");
        }
    }

    @Override
    public void truncate() throws DatabaseException
    {
        // some drivers simply do not have a table (like LayerStorageRscDbDriver)
        if (table != null)
        {
            dbEngine.truncate(table);
        }
    }

    @Override
    public Map<DATA, INIT_MAPS> loadAll(@Nullable LOAD_ALL parentRef) throws DatabaseException
    {
        // fail fast is not configured correctly
        performSanityCheck();

        errorReporter.logTrace("Loading all %ss", table.getName());
        Map<DATA, INIT_MAPS> loadedObjectsMap;
        try
        {
            loadedObjectsMap = dbEngine.loadAll(table, parentRef, this::load);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("Database context does not have enough privileges");
        }
        catch (InvalidNameException | InvalidIpAddressException | ValueOutOfRangeException | MdException |
            ExhaustedPoolException | ValueInUseException exc)
        {
            // TODO improve exception-handling
            throw new DatabaseException("Failed to restore data", exc);
        }
        errorReporter.logTrace("Loaded %4d %ss", getLoadedCount(loadedObjectsMap), table.getName());
        return loadedObjectsMap;
    }

    protected int getLoadedCount(Map<DATA, INIT_MAPS> loadedObjectsMapRef)
    {
        return loadedObjectsMapRef.size();
    }

    public void performSanityCheck()
    {
        for (Column col : table.values())
        {
            if (!setters.containsKey(col))
            {
                throw new ImplementationError(
                    "Missing column-setter for " + table.getName() + "." + col.getName() +
                        " in " + this.getClass().getSimpleName()
                );
            }
        }
    }

    protected <FLAG extends Enum<FLAG> & Flags> StateFlagsPersistence<DATA> generateFlagDriver(
        Column col,
        Class<FLAG> flagsClass
    )
    {
        return dbEngine.generateFlagsDriver(setters, col, flagsClass, this::getId);
    }

    protected <INPUT_TYPE, DB_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateSingleColumnDriver(
        Column col,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToString,
        Function<INPUT_TYPE, DB_TYPE> typeMapper
    )
    {
        return dbEngine.generateSingleColumnDriver(
            setters,
            col,
            typeMapper,
            this::getId,
            dataValueToString,
            Objects::toString
        );
    }

    protected <INPUT_TYPE, DB_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateSingleColumnDriver(
        Column col,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToString,
        Function<INPUT_TYPE, DB_TYPE> typeMapper,
        DataToString<INPUT_TYPE> inputToStringRef
    )
    {
        return dbEngine.generateSingleColumnDriver(
            setters,
            col,
            typeMapper,
            this::getId,
            dataValueToString,
            inputToStringRef
        );
    }

    protected <LIST_TYPE> CollectionDatabaseDriver<DATA, LIST_TYPE> generateCollectionToJsonStringArrayDriver(
        Column col
    )
    {
        return dbEngine.generateCollectionToJsonStringArrayDriver(setters, col, this::getId);
    }

    protected <KEY, VALUE> MapDatabaseDriver<DATA, KEY, VALUE> generateMapToJsonStringArrayDriver(
        Column col
    )
    {
        return dbEngine.generateMapToJsonStringArrayDriver(setters, col, this::getId);
    }

    @SafeVarargs
    protected final <INPUT_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateMultiColumnDriver(
        SingleColumnDatabaseDriver<DATA, INPUT_TYPE>... singleColumnDriversRef
    )
    {
        return new MultiColumnDriver<>(singleColumnDriversRef);
    }

    protected void setColumnSetter(
        Column colRef,
        ExceptionThrowingFunction<DATA, Object, AccessDeniedException> setterRef
    )
    {
        setters.put(colRef, setterRef);
    }

    protected ObjectProtection getObjectProtection(String objProtPath) throws DatabaseException
    {
        return objProtFactory.getInstance(dbCtx, objProtPath, false);
    }

    protected DatabaseType getDbType()
    {
        return dbEngine.getType();
    }

    protected String toString(Map<String, ?> asStrListRef) throws LinStorDBRuntimeException
    {
        try
        {
            return OBJ_MAPPER.writeValueAsString(asStrListRef);
        }
        catch (JsonProcessingException exc)
        {
            throw new LinStorDBRuntimeException("Failed to write json array");
        }
    }

    protected String toString(List<?> asStrListRef) throws LinStorDBRuntimeException
    {
        try
        {
            return OBJ_MAPPER.writeValueAsString(asStrListRef);
        }
        catch (JsonProcessingException exc)
        {
            throw new LinStorDBRuntimeException("Failed to write json array");
        }
    }

    protected byte[] toBlob(List<?> asStrListRef) throws LinStorDBRuntimeException
    {
        try
        {
            return OBJ_MAPPER.writeValueAsBytes(asStrListRef);
        }
        catch (JsonProcessingException exc)
        {
            throw new LinStorDBRuntimeException("Failed to write json array");
        }
    }

    protected abstract @Nullable Pair<DATA, INIT_MAPS> load(
        RawParameters raw,
        LOAD_ALL parentRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException;

    protected abstract String getId(DATA data) throws AccessDeniedException;

    private static class MultiColumnDriver<PARENT, COL_VALUE> implements SingleColumnDatabaseDriver<PARENT, COL_VALUE>
    {
        private final SingleColumnDatabaseDriver<PARENT, COL_VALUE>[] updaterArr;

        MultiColumnDriver(
            SingleColumnDatabaseDriver<PARENT, COL_VALUE>[] updaterArrRef
        )
        {
            updaterArr = updaterArrRef;
        }

        @Override
        public void update(PARENT parentRef, COL_VALUE oldElementRef) throws DatabaseException
        {
            for (SingleColumnDatabaseDriver<PARENT, COL_VALUE> updater : updaterArr)
            {
                updater.update(parentRef, oldElementRef);
            }
        }
    }
}
