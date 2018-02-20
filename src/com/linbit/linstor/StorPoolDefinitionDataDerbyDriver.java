package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.TransactionMgr;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.utils.UuidUtils;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Singleton
public class StorPoolDefinitionDataDerbyDriver implements StorPoolDefinitionDataDatabaseDriver
{
    private static final String TBL_SPD = DerbyConstants.TBL_STOR_POOL_DEFINITIONS;

    private static final String SPD_UUID = DerbyConstants.UUID;
    private static final String SPD_NAME = DerbyConstants.POOL_NAME;
    private static final String SPD_DSP_NAME = DerbyConstants.POOL_DSP_NAME;

    private static final String SPD_SELECT =
        " SELECT " + SPD_UUID + ", " + SPD_NAME + ", " + SPD_DSP_NAME +
        " FROM " + TBL_SPD +
        " WHERE " + SPD_NAME + " = ?";
    private static final String SPD_SELECT_ALL =
        " SELECT " + SPD_UUID + ", " + SPD_NAME + ", " + SPD_DSP_NAME +
        " FROM " + TBL_SPD;
    private static final String SPD_INSERT =
        " INSERT INTO " + TBL_SPD +
        " (" + SPD_UUID + ", " + SPD_NAME + ", " + SPD_DSP_NAME + ")" +
        " VALUES (?, ?, ?)";
    private static final String SPD_DELETE =
        " DELETE FROM " + TBL_SPD +
        " WHERE " + SPD_NAME + " = ?";

    private final ErrorReporter errorReporter;
    private final Map<StorPoolName, StorPoolDefinition> storPoolDfnMap;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;

    @Inject
    public StorPoolDefinitionDataDerbyDriver(
        ErrorReporter errorReporterRef,
        Map<StorPoolName, StorPoolDefinition> storPoolDfnMapRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef
    )
    {
        errorReporter = errorReporterRef;
        storPoolDfnMap = storPoolDfnMapRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
    }

    @Override
    public void create(StorPoolDefinitionData storPoolDefinitionData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Creating StorPoolDefinition %s", getId(storPoolDefinitionData));

        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SPD_INSERT))
        {
            stmt.setBytes(1, UuidUtils.asByteArray(storPoolDefinitionData.getUuid()));
            stmt.setString(2, storPoolDefinitionData.getName().value);
            stmt.setString(3, storPoolDefinitionData.getName().displayValue);
            stmt.executeUpdate();
        }
        errorReporter.logTrace("StorPoolDefinition created %s", getId(storPoolDefinitionData));
    }

    @Override
    public StorPoolDefinitionData load(
        StorPoolName storPoolName,
        boolean logWarnIfNotExists,
        TransactionMgr transMgr
    )
        throws SQLException
    {
        errorReporter.logTrace("Loading StorPoolDefinition %s", getId(storPoolName));

        StorPoolDefinitionData storPoolDefinition = null;
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SPD_SELECT))
        {
            stmt.setString(1, storPoolName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    storPoolDefinition = load(resultSet, transMgr);
                }
                else
                if (logWarnIfNotExists)
                {
                    errorReporter.logWarning(
                        "StorPoolDefinition was not found in the DB %s",
                        getId(storPoolName)
                    );
                }
            }
        }
        return storPoolDefinition;
    }

    public List<StorPoolDefinitionData> loadAll(TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Loading all StorPoolDefinitions");
        List<StorPoolDefinitionData> list = new ArrayList<>();
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SPD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    list.add(
                        load(resultSet, transMgr)
                    );
                }
            }
        }
        errorReporter.logTrace("Loaded %d StorPoolDefinitions", list.size());
        return list;
    }

    public StorPoolDefinitionData load(ResultSet resultSet, TransactionMgr transMgr) throws SQLException
    {
        StorPoolDefinitionData storPoolDefinition = null;
        StorPoolName storPoolName;
        try
        {
            storPoolName = new StorPoolName(resultSet.getString(SPD_DSP_NAME));
        }
        catch (InvalidNameException invalidNameExc)
        {
            throw new LinStorSqlRuntimeException(
                String.format(
                    "A display StorPoolName of a stored StorPoolDefinition in the table %s could not be restored. " +
                        "(invalid display StorPoolName=%s)",
                    TBL_SPD,
                    resultSet.getString(SPD_DSP_NAME)
                ),
                invalidNameExc
            );
        }

        storPoolDefinition = cacheGet(storPoolName);
        if (storPoolDefinition == null)
        {
            UUID uuid = UuidUtils.asUuid(resultSet.getBytes(SPD_UUID));

            ObjectProtection objProt = getObjectProtection(storPoolName, transMgr);

            storPoolDefinition = new StorPoolDefinitionData(
                uuid,
                objProt,
                storPoolName,
                this,
                propsContainerFactory
            );
            errorReporter.logTrace("StorPoolDefinition loaded from DB %s", getId(storPoolName));
        }
        else
        {
            errorReporter.logTrace("StorPoolDefinition loaded from cache %s", getId(storPoolName));
        }
        return storPoolDefinition;
    }

    private ObjectProtection getObjectProtection(StorPoolName storPoolName, TransactionMgr transMgr)
        throws SQLException, ImplementationError
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPathSPD(storPoolName),
            false, // no need to log a warning, as we would fail then anyways
            transMgr
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "StorPoolDefinition's DB entry exists, but is missing an entry in ObjProt table! " +
                getId(storPoolName), null
            );
        }
        return objProt;
    }

    @Override
    public void delete(StorPoolDefinitionData storPoolDefinitionData, TransactionMgr transMgr) throws SQLException
    {
        errorReporter.logTrace("Deleting StorPoolDefinition %s", getId(storPoolDefinitionData));
        try (PreparedStatement stmt = transMgr.dbCon.prepareStatement(SPD_DELETE))
        {
            stmt.setString(1, storPoolDefinitionData.getName().value);
            stmt.executeUpdate();
        }
        errorReporter.logTrace("StorPoolDefinition deleted %s", getId(storPoolDefinitionData));
    }

    private StorPoolDefinitionData cacheGet(StorPoolName sName)
    {
        return (StorPoolDefinitionData) storPoolDfnMap.get(sName);
    }

    private String getId(StorPoolDefinitionData storPoolDefinition)
    {
        return getId(storPoolDefinition.getName().displayValue);
    }

    private String getId(StorPoolName storPoolName)
    {
        return getId(storPoolName.displayValue);
    }

    private String getId(String name)
    {
        return " (StorPoolName=" + name + ")";
    }
}
