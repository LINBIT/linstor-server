package com.linbit.linstor.dbcp;

import com.linbit.linstor.dbdrivers.DatabaseException;

public class DbUtils
{
    private DbUtils()
    {
    }

    public static int parseVersionAsInt(Object versionRef) throws DatabaseException
    {
        int ret;
        if (versionRef instanceof Integer)
        {
            ret = (int) versionRef;
        }
        else if (versionRef instanceof String)
        {
            try
            {
                ret = Integer.parseInt((String) versionRef);
            }
            catch (NumberFormatException nfe)
            {
                throw new DatabaseException("Failed to parse target version: " + versionRef, nfe);
            }
        }
        else
        {
            throw new DatabaseException(
                "Unknown parameter type for target version: " + versionRef + ". Type: " + (versionRef == null ?
                    "<null>" :
                    versionRef.getClass()) + ". Expected type: String or Integer."
            );
        }
        return ret;
    }
}
