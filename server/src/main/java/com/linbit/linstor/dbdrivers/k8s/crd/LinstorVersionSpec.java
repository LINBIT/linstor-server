package com.linbit.linstor.dbdrivers.k8s.crd;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbdrivers.DatabaseTable;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LinstorVersionSpec implements LinstorSpec<LinstorVersionCrd, LinstorVersionSpec>
{
    @JsonIgnore private static final long serialVersionUID = 5539207148044018793L;

    @JsonProperty("version") public final int version;

    @Nullable LinstorVersionCrd parentCrd;

    @JsonCreator
    public LinstorVersionSpec(@JsonProperty("version") int versionRef)
    {
        version = versionRef;
    }

    @Override
    @JsonIgnore
    public @Nullable LinstorVersionCrd getCrd()
    {
        return parentCrd;
    }

    @JsonIgnore
    @Override
    public String getLinstorKey()
    {
        return "version";
    }

    @JsonIgnore
    public static String getYamlLocation()
    {
        return "com/linbit/linstor/dbcp/k8s/crd/LinstorVersion.yaml";
    }

    @Override
    @JsonIgnore
    public Map<String, Object> asRawParameters()
    {
        throw new ImplementationError("Method not supported by LinstorVersion");
    }

    @Override
    @JsonIgnore
    public Object getByColumn(String clmNameStrRef)
    {
        throw new ImplementationError("Method not supported by LinstorVersion");
    }

    @Override
    @JsonIgnore
    public DatabaseTable getDatabaseTable()
    {
        throw new ImplementationError("Method not supported by LinstorVersion");
    }
}
