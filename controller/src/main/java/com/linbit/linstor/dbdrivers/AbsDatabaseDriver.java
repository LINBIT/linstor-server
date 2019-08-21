package com.linbit.linstor.dbdrivers;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Table;
import com.linbit.linstor.dbdrivers.interfaces.GenericDatabaseDriver;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.Pair;

import java.io.IOException;
import java.sql.JDBCType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbsDatabaseDriver<DATA, INIT_MAPS, LOAD_ALL> implements GenericDatabaseDriver<DATA>
{
    protected static final ObjectMapper OBJ_MAPPER = new ObjectMapper();

    private final Table table;
    private final DbEngine dbEngine;
    private final ObjectProtectionDatabaseDriver objProtDriver;

    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;

    public AbsDatabaseDriver(
        Table tableRef,
        DbEngine dbEngineRef,
        ObjectProtectionDatabaseDriver objProtDriverRef
    )
    {
        table = tableRef;
        dbEngine = dbEngineRef;
        objProtDriver = objProtDriverRef;

        setters = new HashMap<>();
    }

    @Override
    public void create(DATA dataRef) throws DatabaseException
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

    @Override
    public void delete(DATA dataRef) throws DatabaseException
    {
        try
        {
            dbEngine.delete(setters, dataRef, table, this::getId);
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError("Database driver does not have enough privileges");
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
            dataValueToString
        );
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
                table.getName() + "'s DB entry exists, but is missing an entry in ObjProt table!" + objProtPath,
                null
            );
        }
        return objProt;
    }

    protected DatabaseType getDbType()
    {
        return dbEngine.getType();
    }

    protected abstract Pair<DATA, INIT_MAPS> load(RawParameters raw) throws DatabaseException, InvalidNameException;

    protected abstract String getId(DATA data);

    public class RawParameters
    {
        private final Object[] rawParameters;

        private RawParameters(Object[] rawParametersRef)
        {
            rawParameters = rawParametersRef;
        }

        @SuppressWarnings("unchecked")
        public <T> T get(Column col)
        {
            return (T) rawParameters[col.getIndex()];
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
                else if (col.getSqlType() == Types.VARCHAR)
                {
                    ret = new ArrayList<>(OBJ_MAPPER.readValue((String) value, List.class));
                }
                else if (col.getSqlType() == Types.BLOB)
                {
                    ret = new ArrayList<>(OBJ_MAPPER.readValue((byte[]) value, List.class));
                }
                else
                {
                    throw new DatabaseException(
                        "Failed to deserialize json array. No handler found for sql type: "
                            + JDBCType.valueOf(col.getSqlType()) +
                            " in table " + table.getName() + ", column " + col.getName()
                    );
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
    }
}
