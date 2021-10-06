package com.linbit.linstor.transaction;

import com.linbit.linstor.dbdrivers.DatabaseTable;

import java.util.function.Function;

public class K8sCrdSchemaUpdateContext
{
    private final Function<DatabaseTable, String> getYamlLocations;
    private final Function<DatabaseTable, String> yamlKindNameFunction;
    private final String targetVersion;

    public K8sCrdSchemaUpdateContext(
        Function<DatabaseTable, String> getYamlLocationsRef,
        Function<DatabaseTable, String> yamlKindNameFunctionRef,
        String targetVersionRef
    )
    {
        getYamlLocations = getYamlLocationsRef;
        yamlKindNameFunction = yamlKindNameFunctionRef;
        targetVersion = targetVersionRef;
    }

    public Function<DatabaseTable, String> getGetYamlLocations()
    {
        return getYamlLocations;
    }

    public Function<DatabaseTable, String> getGetYamlKindNameFunction()
    {
        return yamlKindNameFunction;
    }

    public String getTargetVersion()
    {
        return targetVersion;
    }
}
