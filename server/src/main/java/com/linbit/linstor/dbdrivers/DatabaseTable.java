package com.linbit.linstor.dbdrivers;

public interface DatabaseTable
{
    /**
     * Returns all columns of the current table
     */
    Column[] values();

    /**
     * Returns the name of the current table
     */
    String getName();

    public interface Column
    {
        String getName();

        int getSqlType();

        boolean isPk();

        boolean isNullable();

        DatabaseTable getTable();
    }
}
