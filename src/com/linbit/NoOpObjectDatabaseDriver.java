package com.linbit;

import java.sql.SQLException;

public class NoOpObjectDatabaseDriver<PARENT, ELEMENT> implements SingleColumnDatabaseDriver<PARENT, ELEMENT>
{
    @Override
    public void update(PARENT parent, ELEMENT element) throws SQLException
    {
        // no-op
    }
}
