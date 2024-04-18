package com.linbit.linstor.dbdrivers.sql;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine.DataToString;
import com.linbit.linstor.dbdrivers.interfaces.updater.MapDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ExceptionThrowingFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

class SQLMapToJsonDriver<DATA, KEY, VALUE> implements MapDatabaseDriver<DATA, KEY, VALUE>
{
    private final ErrorReporter errorReporter;
    private final SQLEngine sqlEngine;
    private final Column columnToUpdate;
    private final String updateStatement;
    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;
    private final DataToString<DATA> dataToString;

    private final DatabaseTable table;
    private final ExceptionThrowingFunction<DATA, Object, AccessDeniedException> columnSetter;

    SQLMapToJsonDriver(
        SQLEngine sqlEngineRef,
        ErrorReporter errorReporterRef,
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column columnToUpdateRef,
        DataToString<DATA> dataToStringRef
    )
    {
        sqlEngine = sqlEngineRef;
        errorReporter = errorReporterRef;
        setters = settersRef;
        columnToUpdate = columnToUpdateRef;
        dataToString = dataToStringRef;
        updateStatement = sqlEngineRef.generateUpdateStatement(columnToUpdateRef);

        table = columnToUpdateRef.getTable();
        columnSetter = setters.get(columnToUpdate);
    }

    @Override
    public void insert(DATA parentRef, Map<KEY, VALUE> backingMapRef, KEY keyRef, VALUE valueRef)
        throws DatabaseException
    {
        update(parentRef, backingMapRef);
    }

    @Override
    public void update(DATA parentRef, Map<KEY, VALUE> backingMapRef, KEY keyRef, VALUE oldValueRef, VALUE newValueRef)
        throws DatabaseException
    {
        update(parentRef, backingMapRef);
    }

    @Override
    public void delete(DATA parentRef, Map<KEY, VALUE> backingMapRef, KEY keyRef, VALUE valueRef)
        throws DatabaseException
    {
        update(parentRef, backingMapRef);
    }

    private void update(DATA data, Map<KEY, VALUE> backingMapRef)
        throws DatabaseException
    {
        try (PreparedStatement stmt = sqlEngine.getConnection().prepareStatement(updateStatement))
        {
            String inlineId = dataToString.toString(data);
            errorReporter.logTrace(
                "Updating %s's %s to %s of %s",
                table.getName(),
                columnToUpdate.getName(),
                backingMapRef.toString(),
                inlineId
            );

            stmt.setObject(1, columnSetter.accept(data));
            sqlEngine.setPrimaryValues(setters, stmt, 2, table, data);

            stmt.executeUpdate();
            errorReporter.logTrace(
                "%s's %s updated to %s %s",
                table.getName(),
                columnToUpdate.getName(),
                backingMapRef.toString(),
                inlineId
            );
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
}
