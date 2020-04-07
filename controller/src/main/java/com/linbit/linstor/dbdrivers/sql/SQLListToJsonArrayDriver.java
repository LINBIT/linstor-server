package com.linbit.linstor.dbdrivers.sql;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine.DataToString;
import com.linbit.linstor.dbdrivers.interfaces.updater.CollectionDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ExceptionThrowingFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

class SQLListToJsonArrayDriver<DATA, LIST_TYPE> implements CollectionDatabaseDriver<DATA, LIST_TYPE>
{
    private final ErrorReporter errorReporter;
    private final SQLEngine sqlEngine;
    private final Column columnToUpdate;
    private final String updateStatement;
    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;
    private final DataToString<DATA> dataToString;

    private final DatabaseTable table;
    private final ExceptionThrowingFunction<DATA, Object, AccessDeniedException> columnSetter;

    SQLListToJsonArrayDriver(
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
    public void insert(DATA dataRef, LIST_TYPE elementRef, Collection<LIST_TYPE> backingCollectionRef)
        throws DatabaseException
    {
        update(dataRef, backingCollectionRef);
    }

    @Override
    public void remove(DATA dataRef, LIST_TYPE elementRef, Collection<LIST_TYPE> backingCollectionRef)
        throws DatabaseException
    {
        update(dataRef, backingCollectionRef);
    }

    private void update(DATA data, Collection<LIST_TYPE> backingCollection)
        throws DatabaseException
    {
        try (PreparedStatement stmt = sqlEngine.getConnection().prepareStatement(updateStatement))
        {
            String inlineId = dataToString.toString(data);
            errorReporter.logTrace(
                "Updating %s's %s to %s of %s",
                table.getName(),
                columnToUpdate.getName(),
                backingCollection.toString(),
                inlineId
            );

            stmt.setObject(1, columnSetter.accept(data));
            sqlEngine.setPrimaryValues(setters, stmt, 2, table, data);

            stmt.executeUpdate();
            errorReporter.logTrace(
                "%s's %s updated to %s %s",
                table.getName(),
                columnToUpdate.getName(),
                backingCollection.toString(),
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
