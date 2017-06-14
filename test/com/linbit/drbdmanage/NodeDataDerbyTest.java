package com.linbit.drbdmanage;

import java.sql.SQLException;

import com.linbit.drbdmanage.security.DerbyBase;

public class NodeDataDerbyTest extends DerbyBase
{
    private static String[] createTables;
    private static String[] defaultValues;
    private static String[] truncateTables;
    private static String[] dropTables;

    public NodeDataDerbyTest() throws SQLException
    {
        super(createTables, defaultValues, truncateTables, dropTables);
    }

}
