package com.linbit.drbdmanage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.derby.DerbyConstants;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.logging.ErrorReporter;
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

    private final ErrorReporter errorReporter;
    private final Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;

    public StorPoolDefinitionDataDerbyDriver(
        ErrorReporter errorReporterRef,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMapRef
    )
    {
        errorReporter = errorReporterRef;
        storPoolDfnMap = storPoolDfnMapRef;
    }

    @Override
    public void create(StorPoolDefinitionData storPoolDefinitionData, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SPD_INSERT);
        stmt.setBytes(1, UuidUtils.asByteArray(storPoolDefinitionData.getUuid()));
        stmt.setString(2, storPoolDefinitionData.getName().value);
        stmt.setString(3, storPoolDefinitionData.getName().displayValue);
        stmt.executeUpdate();
        stmt.close();

        cache(storPoolDefinitionData);
    }

    @Override
    public StorPoolDefinitionData load(StorPoolName storPoolName, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SPD_SELECT);
        stmt.setString(1, storPoolName.value);
        ResultSet resultSet = stmt.executeQuery();

        StorPoolDefinitionData spdd = cacheGet(storPoolName);
        if (spdd == null)
        {
            if (resultSet.next())
            {
                try
                {
                    storPoolName = new StorPoolName(resultSet.getString(SPD_DSP_NAME));
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


                UUID id = UuidUtils.asUuid(resultSet.getBytes(SPD_UUID));

                ObjectProtectionDatabaseDriver objProtDriver = DrbdManage.getObjectProtectionDatabaseDriver(
                );
                ObjectProtection objProt = objProtDriver.loadObjectProtection(
                    ObjectProtection.buildPathSPD(storPoolName),
                    transMgr
                );

                spdd = new StorPoolDefinitionData(id, objProt, storPoolName);
                cache(spdd);
            }
        }
        else
        {
            // TODO: warn that no entry was found
        }
        resultSet.close();
        stmt.close();
        return spdd;
    }

    @Override
    public void delete(StorPoolDefinitionData storPoolDefinitionData, TransactionMgr transMgr) throws SQLException
    {
        PreparedStatement stmt = transMgr.dbCon.prepareStatement(SPD_DELETE);
        stmt.setString(1, storPoolDefinitionData.getName().value);
        stmt.executeUpdate();
        stmt.close();
    }

    private void cache(StorPoolDefinitionData spdd)
    {
        storPoolDfnMap.put(spdd.getName(), spdd);
    }

    private StorPoolDefinitionData cacheGet(StorPoolName sName)
    {
        return (StorPoolDefinitionData) storPoolDfnMap.get(sName);
    }
}
