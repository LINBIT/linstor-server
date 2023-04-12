package com.linbit.linstor.dbdrivers;

import com.linbit.ExhaustedPoolException;
import com.linbit.ImplementationError;
import com.linbit.InvalidIpAddressException;
import com.linbit.InvalidNameException;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.drbd.md.MdException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine.DataToString;
import com.linbit.linstor.dbdrivers.etcd.ETCDEngine;
import com.linbit.linstor.dbdrivers.interfaces.GenericDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.sql.SQLEngine;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbsDatabaseDriver<DATA, INIT_MAPS, LOAD_ALL>
    implements GenericDatabaseDriver<DATA>, ControllerDatabaseDriver<DATA, INIT_MAPS, LOAD_ALL>
{
    protected static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private final ErrorReporter errorReporter;
    /**
     * The database table to read from and write to.
     * Can be null as some linstor objects (especially layer-related) are skipping "inherited" tables. That means they
     * do need a (rscData-) driver without having a dedicated table to work with, but are still needed in order to
     * correctly load vlmData entries from existing database tables
     */
    protected final DatabaseTable table;
    private final DbEngine dbEngine;
    private final ObjectProtectionDatabaseDriver objProtDriver;

    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;

    public AbsDatabaseDriver(
        ErrorReporter errorReporterRef,
        @Nullable DatabaseTable tableRef,
        DbEngine dbEngineRef,
        ObjectProtectionDatabaseDriver objProtDriverRef
    )
    {
        errorReporter = errorReporterRef;
        table = tableRef;
        dbEngine = dbEngineRef;
        objProtDriver = objProtDriverRef;

        setters = new HashMap<>();
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
    public void delete(DATA dataRef) throws DatabaseException
    {
        try
        {
            if (table != null)
            {
                // some drivers simply do not have a table (like LayerStorageRscDbDriver)
                dbEngine.delete(setters, dataRef, table, this::getId);
            }
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("Database driver does not have enough privileges");
        }
    }

    @Override
    public Map<DATA, INIT_MAPS> loadAll(LOAD_ALL parentRef) throws DatabaseException
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
        errorReporter.logTrace("Loaded %d %ss", loadedObjectsMap.size(), table.getName());
        return loadedObjectsMap;
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

    protected void setColumnSetter(
        Column colRef,
        ExceptionThrowingFunction<DATA, Object, AccessDeniedException> setterRef
    )
    {
        setters.put(colRef, setterRef);
    }

    protected ObjectProtection getObjectProtection(String objProtPath) throws DatabaseException
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            objProtPath,
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                table.getName() + "'s DB entry exists, but is missing an entry in ObjProt table! " + objProtPath,
                null
            );
        }
        return objProt;
    }

    protected DatabaseType getDbType()
    {
        return dbEngine.getType();
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

    protected abstract Pair<DATA, INIT_MAPS> load(
        RawParameters raw,
        LOAD_ALL parentRef
    )
        throws DatabaseException, InvalidNameException, ValueOutOfRangeException, InvalidIpAddressException,
        MdException, ExhaustedPoolException, ValueInUseException, RuntimeException, AccessDeniedException;

    protected abstract String getId(DATA data) throws AccessDeniedException;

    /**
     * This class is basically only a wrapper for an Object[]. {@link ETCDEngine} or {@link SQLEngine}
     * already have written their java-raw types in this Object[] ({@link ETCDEngine} only {@link String}s
     * but {@link SQLEngine} the types defined in {@link GeneratedDatabaseTables}).
     */
    public static class RawParameters
    {
        private final DatabaseTable table;
        private final Map<String, Object> rawParameters;
        private final DatabaseType dbType;

        public RawParameters(DatabaseTable tableRef, Map<String, Object> rawParametersRef, DatabaseType dbTypeRef)
        {
            table = tableRef;
            rawParameters = rawParametersRef;
            dbType = dbTypeRef;
        }

        /**
         * ETCD always returns Strings for {@link #get(Column)}. This method however will call for ETCD the
         * {@link #getValueFromEtcd(Column)} before returning the value.
         * As a result, the value will be parsed based on the {@link Column#getSqlType()}.
         *
         * For non-ETCD engines, this method is a simple delegate for {@link #get(Column)}.
         */
        public <T> T getParsed(Column col)
        {
            T ret;
            switch (dbType)
            {
                case SQL:
                case K8S_CRD:
                    ret = get(col);
                    break;
                case ETCD:
                    ret = getValueFromEtcd(col);
                    break;
                default:
                    throw new ImplementationError("Unknown db type: " + dbType);
            }
            return ret;
        }

        /**
         * This method does the same as {@link #build(Column, Class)}, but instead of depending on {@link #get(Column)}
         * this method calls {@link #getParsed(Column)}.
         */
        public <T, R, EXC extends Exception> R buildParsed(
            Column col,
            ExceptionThrowingFunction<T, R, EXC> func
        )
            throws EXC
        {
            T data = getParsed(col);
            R ret = null;
            if (data != null)
            {
                ret = func.accept(data);
            }
            return ret;
        }

        @SuppressWarnings("unchecked")
        private <T> T getValueFromEtcd(Column col) throws ImplementationError
        {
            T ret;
            String etcdVal = (String) rawParameters.get(col.getName());
            switch (col.getSqlType())
            {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARBINARY:
                    ret = (T) etcdVal;
                    break;
                case Types.BIT:
                    ret = (T) ((Boolean) Boolean.parseBoolean(etcdVal));
                    break;
                case Types.TINYINT:
                    ret = (T) ((Byte) Byte.parseByte(etcdVal));
                    break;
                case Types.SMALLINT:
                    ret = (T) ((Short) Short.parseShort(etcdVal));
                    break;
                case Types.INTEGER:
                    ret = (T) ((Integer) Integer.parseInt(etcdVal));
                    break;
                case Types.BIGINT:
                    ret = (T) ((Long) Long.parseLong(etcdVal));
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    ret = (T) ((Float) Float.parseFloat(etcdVal));
                    break;
                case Types.DOUBLE:
                    ret = (T) ((Double) Double.parseDouble(etcdVal));
                    break;
                default:
                    throw new ImplementationError("Unhandled SQL type: " + col.getSqlType());
            }
            return ret;
        }

        @SuppressWarnings("unchecked")
        public <T> T get(Column col)
        {
            return (T) rawParameters.get(col.getName());
        }

        public <T, R, EXC extends Exception> R build(
            Column col,
            ExceptionThrowingFunction<T, R, EXC> func
        )
            throws EXC
        {
            T data = get(col);
            R ret = null;
            if (data != null)
            {
                ret = func.accept(data);
            }
            return ret;
        }

        public <R extends Enum<R>> R build(Column col, Class<R> eType)
            throws IllegalArgumentException
        {
            String data = get(col);
            R ret = null;
            if (data != null)
            {
                ret = Enum.valueOf(eType, data);
            }
            return ret;
        }

        public List<String> getAsStringList(Column col) throws DatabaseException
        {
            List<String> ret;
            try
            {
                Object value = get(col);
                if (value == null)
                {
                    ret = null;
                }
                else
                {
                    int colSqlType = col.getSqlType();
                    if (colSqlType == Types.VARCHAR || colSqlType == Types.CLOB)
                    {
                        ret = new ArrayList<>(OBJ_MAPPER.readValue((String) value, List.class));
                    }
                    else if (colSqlType == Types.BLOB)
                    {
                        ret = new ArrayList<>(OBJ_MAPPER.readValue((byte[]) value, List.class));
                    }
                    else
                    {
                        throw new DatabaseException(
                            "Failed to deserialize json array. No handler found for sql type: " +
                                JDBCType.valueOf(colSqlType) +
                                " in table " + table.getName() + ", column " + col.getName()
                        );
                    }
                }
            }
            catch (IOException exc)
            {
                throw new DatabaseException(
                    "Failed to deserialize json array. Table: " + table.getName() + ", column: " + col.getName(),
                    exc
                );
            }

            return ret;
        }

        public Short etcdGetShort(Column column)
        {
            return this.<String, Short, RuntimeException>build(column, Short::parseShort);
        }

        public Integer etcdGetInt(Column column)
        {
            return this.<String, Integer, RuntimeException>build(column, Integer::parseInt);
        }

        public Long etcdGetLong(Column column)
        {
            return this.<String, Long, RuntimeException>build(column, Long::parseLong);
        }

        public Boolean etcdGetBoolean(Column column)
        {
            return this.<String, Boolean, RuntimeException>build(column, Boolean::parseBoolean);
        }
    }
}
