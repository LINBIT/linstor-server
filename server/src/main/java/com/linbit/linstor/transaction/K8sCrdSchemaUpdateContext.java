package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseTable;

import java.util.function.Function;

public class K8sCrdSchemaUpdateContext
{
    private final Function<DatabaseTable, String> getYamlLocations;
    private final String rollbackYamlLocation;

    public K8sCrdSchemaUpdateContext(
        Function<DatabaseTable, String> getYamlLocationsRef,
        String rollbackYamlLocationRef
    )
    {
        super();
        getYamlLocations = getYamlLocationsRef;
        rollbackYamlLocation = rollbackYamlLocationRef;
    }

    public Function<DatabaseTable, String> getGetYamlLocations()
    {
        return getYamlLocations;
    }

    public String getRollbackYamlLocation()
    {
        return rollbackYamlLocation;
    }
}
