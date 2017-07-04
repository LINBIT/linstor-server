package com.linbit.drbdmanage.dbdrivers.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

import com.linbit.drbdmanage.StorPoolDefinitionData;

public interface StorPoolDefinitionDataDatabaseDriver
{
    public void create(Connection con, StorPoolDefinitionData storPoolDefinitionData)
        throws SQLException;

    public StorPoolDefinitionData load(Connection con)
        throws SQLException;

    public void delete(Connection con)
        throws SQLException;
}
