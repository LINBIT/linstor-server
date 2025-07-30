package com.linbit.linstor.dbcp.migration;

import com.linbit.linstor.LinStorRuntimeException;

import java.util.regex.Pattern;

public class LinstorMigrationVersion implements Comparable<LinstorMigrationVersion>
{
    // pattern for e.g.: 2021.04.29.12.00
    public static final Pattern VERSION_PATTERN = Pattern.compile("^\\d{4}(?:\\.\\d{2}){4}$");
    private final String versionString;

    public LinstorMigrationVersion(String versionStringRef)
    {
        versionString = versionStringRef;
    }

    public static LinstorMigrationVersion fromVersion(String versionStringRef)
    {
        var matcher = VERSION_PATTERN.matcher(versionStringRef);
        if (!matcher.matches())
        {
            throw new LinStorRuntimeException("Wrong migrate version format: " + versionStringRef);
        }
        return new LinstorMigrationVersion(versionStringRef);
    }

    @Override
    public int hashCode()
    {
        return versionString.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        boolean equal = false;
        if (other instanceof LinstorMigrationVersion)
        {
            equal = versionString.equals(((LinstorMigrationVersion) other).versionString);
        }
        return equal;
    }

    @Override
    public int compareTo(LinstorMigrationVersion other)
    {
        return versionString.compareTo(other.versionString);
    }

    @Override
    public String toString()
    {
        return versionString;
    }
}
