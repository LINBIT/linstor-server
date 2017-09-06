package com.linbit.drbdmanage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.PrimaryKey;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.security.ObjectProtectionDatabaseDriver;
import com.linbit.utils.UuidUtils;

public class StorPoolDefinitionDataDerbyDriver implements StorPoolDefinitionDataDatabaseDriver
{
    private static final String TBL_SPD = DerbyConstants.TBL_STOR_POOL_DEFINITIONS;

    private static final String SPD_UUID = DerbyConstants.UUID;
    private static final String SPD_NAME = DerbyConstants.POOL_NAME;
    private static final String SPD_DSP_NAME = DerbyConstants.POOL_DSP_NAME;

    private static final String SPD_INSERT =
        " INSERT INTO " + TBL_SPD +
        " VALUES (?, ?, ?)";
    private static final String SPD_SELECT =
        " SELECT " + SPD_UUID + ", " + SPD_NAME + ", " + SPD_DSP_NAME +
        " FROM " + TBL_SPD +
        " WHERE " + SPD_NAME + " = ?";
    private static final String SPD_DELETE =
        " DELETE FROM " + TBL_SPD +
        " WHERE " + SPD_NAME + " = ?";

    private static Hashtable<PrimaryKey, StorPoolDefinitionData> spDfnCache = new Hashtable<>();

    private StorPoolName spddName;
    private boolean spddNameLoaded = false;

    public StorPoolDefinitionDataDerbyDriver(StorPoolName nameRef)
    {
        spddName = nameRef;
    }

    @Override
    public void create(Connection con, StorPoolDefinitionData spdd) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(SPD_INSERT);
        stmt.setBytes(1, UuidUtils.asByteArray(spdd.getUuid()));
        stmt.setString(2, spdd.getName().value);
        stmt.setString(3, spdd.getName().displayValue);
        stmt.executeUpdate();
        stmt.close();

        cache(spdd);
    }

    @Override
    public StorPoolDefinitionData load(Connection con) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(SPD_SELECT);
        stmt.setString(1, spddName.value);
        ResultSet resultSet = stmt.executeQuery();

        StorPoolDefinitionData spdd = cacheGet(spddName);
        if (spdd == null)
        {
            if (resultSet.next())
            {
                if (!spddNameLoaded)
                {
                    try
                    {
                        spddName = new StorPoolName(resultSet.getString(SPD_DSP_NAME));
                    }
                    catch (InvalidNameException invalidNameExc)
                    {
                        resultSet.close();
                        stmt.close();
                        throw new ImplementationError(
                            "The display name of a valid StorPoolName could not be restored",
                            invalidNameExc
                        );
                    }
                }


                UUID id = UuidUtils.asUuid(resultSet.getBytes(SPD_UUID));

                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                    ObjectProtection.buildPathSPD(spddName)
                );
                ObjectProtection objProt = objProtDriver.loadObjectProtection(con);

                spdd = new StorPoolDefinitionData(id, objProt, spddName);
                if (!cache(spdd))
                {
                    spdd = cacheGet(spddName);
                }
                else
                {
                    // restore references
                }
            }
        }
        else
        {
            if (!resultSet.next())
            {
                // XXX: user deleted db entry during runtime - throw exception?
                // or just remove the item from the cache + detach item from parent (if needed) + warn the user?
            }
        }
        resultSet.close();
        stmt.close();
        return spdd;
    }

    @Override
    public void delete(Connection con) throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement(SPD_DELETE);
        stmt.setString(1, spddName.value);
        stmt.executeUpdate();
        stmt.close();

        cacheRemove(spddName);
    }

    private synchronized static boolean cache(StorPoolDefinitionData spdd)
    {
        PrimaryKey pk = new PrimaryKey(spdd.getName().value);
        boolean contains = spDfnCache.containsKey(pk);
        if (!contains)
        {
            spDfnCache.put(pk, spdd);
        }
        return !contains;
    }

    private static StorPoolDefinitionData cacheGet(StorPoolName sName)
    {
        return spDfnCache.get(new PrimaryKey(sName.value));
    }

    private synchronized static void cacheRemove(StorPoolName sName)
    {
        spDfnCache.remove(new PrimaryKey(sName.value));
    }

    /**
     * this method should only be called by tests or if you want a full-reload from the database
     */
    static synchronized void clearCache()
    {
        spDfnCache.clear();
    }
}
