package com.linbit.linstor.dbdrivers.sql;

import com.linbit.ImplementationError;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine.DataToString;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.utils.ExceptionThrowingFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.function.Function;

class SQLSingleColumnDriver<DATA, INPUT_TYPE, DB_TYPE> implements SingleColumnDatabaseDriver<DATA, INPUT_TYPE>
{
    private final SQLEngine sqlEngine;
    private final ErrorReporter errorReporter;
    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;
    private final Column colToUpdate;
    private final DataToString<DATA> dataToString;
    private final ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToString;
    private final Function<INPUT_TYPE, DB_TYPE> mapper;

    private final DatabaseTable table;
    private final String updateStatement;

    private final DataToString<INPUT_TYPE> inputToStringFct;

    SQLSingleColumnDriver(
        SQLEngine sqlEngineRef,
        ErrorReporter errorReporterRef,
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column colToUpdateRef,
        Function<INPUT_TYPE, DB_TYPE> mapperRef,
        DataToString<DATA> dataToStringRef,
        ExceptionThrowingFunction<DATA, String, AccessDeniedException> dataValueToStringRef,
        DataToString<INPUT_TYPE> inputToStringRef
    )
    {
        sqlEngine = sqlEngineRef;
        errorReporter = errorReporterRef;
        setters = settersRef;
        colToUpdate = colToUpdateRef;
        dataToString = dataToStringRef;
        dataValueToString = dataValueToStringRef;
        mapper = mapperRef;
        inputToStringFct = inputToStringRef;

        table = colToUpdateRef.getTable();
        updateStatement = sqlEngine.generateUpdateStatement(colToUpdateRef);
    }

    @Override
    public void update(DATA parentRef, INPUT_TYPE elementRef) throws DatabaseException
    {
        try
        {
            if (elementRef instanceof byte[])
            {
                errorReporter.logTrace(
                    "Updating %s's %s from [%s] to [%s] %s",
                    table.getName(),
                    colToUpdate.getName(),
                    dataValueToString.accept(parentRef) == null ? "null" : "<Array of bytes>",
                    elementRef == null ? "null" : "<Array of bytes>",
                    dataToString.toString(parentRef)
                );
            }
            else
            {
                errorReporter.logTrace(
                    "Updating %s's %s from [%s] to [%s] %s",
                    table.getName(),
                    colToUpdate.getName(),
                    dataValueToString.accept(parentRef),
                    elementRef == null ? "null" : inputToStringFct.toString(elementRef),
                    dataToString.toString(parentRef)
                );
            }
            try (PreparedStatement stmt = sqlEngine.getConnection().prepareStatement(updateStatement))
            {
                int idx = fillSetter(stmt, 1, elementRef);
                sqlEngine.setPrimaryValues(setters, stmt, idx, table, parentRef);

                stmt.executeUpdate();
            }
            if (elementRef instanceof byte[])
            {
                errorReporter.logTrace(
                    "%s's %s updated from [%s] to [%s] %s",
                    table.getName(),
                    colToUpdate.getName(),
                    dataValueToString.accept(parentRef) == null ? "null" : "<Array of bytes>",
                    elementRef == null ? "null" : "<Array of bytes>",
                    dataToString.toString(parentRef)
                );
            }
            else
            {
                errorReporter.logTrace(
                    "%s's %s updated from [%s] to [%s] %s",
                    table.getName(),
                    colToUpdate.getName(),
                    dataValueToString.accept(parentRef),
                    elementRef == null ? "null" : inputToStringFct.toString(elementRef),
                    dataToString.toString(parentRef)
                );
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        catch (AccessDeniedException accDeniedExc)
        {
            DatabaseLoader.handleAccessDeniedException(accDeniedExc);
        }
    }

    /**
     * This method performs the necessary stmt.set* method-calls
     *
     * @param stmt
     * @param startIdx
     * @param element
     * @return the index of the next column which was not yet set.
     * @throws SQLException
     */
    protected int fillSetter(PreparedStatement stmt, int startIdx, INPUT_TYPE element)
        throws SQLException
    {
        DB_TYPE data = mapper.apply(element);
        if (colToUpdate.getSqlType() == Types.BLOB)
        {
            if (data == null)
            {
                stmt.setNull(startIdx, colToUpdate.getSqlType());
            }
            else if (data instanceof byte[])
            {
                // BLOB needs special treatment as i.e. PSQL does not support setting byte[] as a blob
                byte[] arr = (byte[]) data;
                stmt.setBytes(startIdx, arr);
            }
            else
            {
                throw new ImplementationError("Invalid class (" + data.getClass() + ") for BLOB type");
            }
        }
        else
        {
            stmt.setObject(startIdx, data, colToUpdate.getSqlType());
        }
        return startIdx + 1;
    }
}
