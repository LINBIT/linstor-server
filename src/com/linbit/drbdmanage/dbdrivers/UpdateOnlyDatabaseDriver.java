package com.linbit.drbdmanage.dbdrivers;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.ImplementationError;
import com.linbit.ObjectDatabaseDriver;

public abstract class UpdateOnlyDatabaseDriver<T> implements ObjectDatabaseDriver<T>
{
    protected String errorFormatInsert = "The %s can only be updated, not (newly) inserted!";
    protected String errorFormatDelete = "The %s can only be updated, not deleted!";

    protected String columnName;

    public UpdateOnlyDatabaseDriver(String columnName)
    {
        this.columnName = columnName;
    }

    @Override
    public void insert(Connection con, T elem) throws SQLException
    {
        throw new ImplementationError(
            String.format(errorFormatInsert, columnName),
            null
        );
    }

    @Override
    public void delete(Connection con, T elem) throws SQLException
    {
        throw new ImplementationError(
            String.format(errorFormatInsert, columnName),
            null
        );
    }
}
