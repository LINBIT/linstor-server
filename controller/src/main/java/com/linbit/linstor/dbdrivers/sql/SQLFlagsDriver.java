package com.linbit.linstor.dbdrivers.sql;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.DatabaseLoader;
import com.linbit.linstor.dbdrivers.DatabaseTable;
import com.linbit.linstor.dbdrivers.DatabaseTable.Column;
import com.linbit.linstor.dbdrivers.DbEngine.DataToString;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.Flags;
import com.linbit.linstor.stateflags.FlagsHelper;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.utils.ExceptionThrowingFunction;
import com.linbit.utils.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

class SQLFlagsDriver<DATA, FLAG extends Enum<FLAG> & Flags> implements StateFlagsPersistence<DATA>
{
    private final ErrorReporter errorReporter;
    private final SQLEngine sqlEngine;
    private final Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> setters;
    private final Column flagColumn;
    private final DataToString<DATA> dataToString;

    private final DatabaseTable table;
    private final Class<FLAG> flagsClass;
    private final String updateStatement;

    SQLFlagsDriver(
        SQLEngine sqlEngineRef,
        ErrorReporter errorReporterRef,
        Map<Column, ExceptionThrowingFunction<DATA, Object, AccessDeniedException>> settersRef,
        Column flagColumnRef,
        Class<FLAG> flagsClassRef,
        DataToString<DATA> dataToStringRef
    )
    {
        sqlEngine = sqlEngineRef;
        errorReporter = errorReporterRef;
        setters = settersRef;
        dataToString = dataToStringRef;
        updateStatement = sqlEngineRef.generateUpdateStatement(flagColumnRef);

        table = flagColumnRef.getTable();
        flagColumn = flagColumnRef;
        flagsClass = flagsClassRef;
    }

    @Override
    public void persist(DATA data, long oldFlagBits, long newFlagBits) throws DatabaseException
    {
        try (PreparedStatement stmt = sqlEngine.getConnection().prepareStatement(updateStatement))
        {
            String fromFlags = StringUtils.join(
                FlagsHelper.toStringList(flagsClass, oldFlagBits),
                ", "
            );
            String toFlags = StringUtils.join(
                FlagsHelper.toStringList(flagsClass, newFlagBits),
                ", "
            );
            String inlineId = dataToString.toString(data);

            errorReporter
                .logTrace(
                    "Updating %s's flags from [%s] to [%s] %s",
                    table.getName(),
                    fromFlags,
                    toFlags,
                    inlineId
                );
            stmt.setLong(1, newFlagBits);
            sqlEngine.setPrimaryValues(setters, stmt, 2, table, data);

            stmt.executeUpdate();

            errorReporter
                .logTrace(
                    "%s's flags updated from [%s] to [%s] %s",
                    table.getName(),
                    fromFlags,
                    toFlags,
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
