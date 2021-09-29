package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseTable;

import java.util.function.Function;

public class K8sCrdSchemaUpdateContext
{
    private final Function<DatabaseTable, String> getYamlLocations;

    public K8sCrdSchemaUpdateContext(
        Function<DatabaseTable, String> getYamlLocationsRef
    )
    {
        getYamlLocations = getYamlLocationsRef;
    }

    public Function<DatabaseTable, String> getGetYamlLocations()
    {
        return getYamlLocations;
    }
}
