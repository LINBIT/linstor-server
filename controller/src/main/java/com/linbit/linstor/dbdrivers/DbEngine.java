package com.linbit.linstor.dbdrivers;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseDriverInfo.DatabaseType;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Column;
import com.linbit.linstor.dbdrivers.GeneratedDatabaseTables.Table;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.ExceptionThrowingFunction;

import java.util.Map;
import java.util.function.Function;

public interface DbEngine
{
    interface DataToString<DATA>
    {
        String toString(DATA data);
    }

    DatabaseType getType();

    <DATA, FLAG extends Enum<FLAG> & Flags> StateFlagsPersistence<DATA> generateFlagsDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column colRef,
        Class<FLAG> flagsClassRef,
        DataToString<DATA> idFormatterRef
    );

    <DATA, INPUT_TYPE, DB_TYPE> SingleColumnDatabaseDriver<DATA, INPUT_TYPE> generateSingleColumnDriver(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        Column colRef,
        Function<INPUT_TYPE, DB_TYPE> typeMapperRef,
        DataToString<DATA> dataToStringRef,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToStringRef
    );

    <DATA> void create(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA dataRef,
        Table table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException;

    <DATA> void delete(
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters,
        DATA dataRef,
        Table table,
        DataToString<DATA> dataToString
    )
        throws DatabaseException, AccessDeniedException;
}
