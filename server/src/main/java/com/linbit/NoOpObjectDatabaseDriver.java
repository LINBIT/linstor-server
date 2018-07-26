package com.linbit;

public class NoOpObjectDatabaseDriver<PARENT, ELEMENT> implements SingleColumnDatabaseDriver<PARENT, ELEMENT>
{
    @Override
    public void update(PARENT parent, ELEMENT element)
    {
        // no-op
    }
}
