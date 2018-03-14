package com.linbit.linstor;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.derby.DerbyConstants;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.UuidUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
    private final CoreModule.StorPoolDefinitionMap storPoolDfnMap;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgr> transMgrProvider;

    @Inject
    public StorPoolDefinitionDataDerbyDriver(
        ErrorReporter errorReporterRef,
        @Uninitialized CoreModule.StorPoolDefinitionMap storPoolDfnMapRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgr> transMgrProviderRef
    )
    {
        errorReporter = errorReporterRef;
        storPoolDfnMap = storPoolDfnMapRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(StorPoolDefinitionData storPoolDefinitionData) throws SQLException
    {
        errorReporter.logTrace("Creating StorPoolDefinition %s", getId(storPoolDefinitionData));

        try (PreparedStatement stmt = getConnection().prepareStatement(SPD_INSERT))
        {
            stmt.setString(1, storPoolDefinitionData.getUuid().toString());
            stmt.setString(2, storPoolDefinitionData.getName().value);
            stmt.setString(3, storPoolDefinitionData.getName().displayValue);
            stmt.executeUpdate();
        }
        errorReporter.logTrace("StorPoolDefinition created %s", getId(storPoolDefinitionData));
    }

    @Override
    public StorPoolDefinitionData load(StorPoolName storPoolName, boolean logWarnIfNotExists)
        throws SQLException
    {
        errorReporter.logTrace("Loading StorPoolDefinition %s", getId(storPoolName));

        StorPoolDefinitionData storPoolDefinition = null;
        try (PreparedStatement stmt = getConnection().prepareStatement(SPD_SELECT))
        {
            stmt.setString(1, storPoolName.value);
            try (ResultSet resultSet = stmt.executeQuery())
            {
                if (resultSet.next())
                {
                    storPoolDefinition = load(resultSet);
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

    public List<StorPoolDefinitionData> loadAll() throws SQLException
    {
        errorReporter.logTrace("Loading all StorPoolDefinitions");
        List<StorPoolDefinitionData> list = new ArrayList<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(SPD_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    list.add(
                        load(resultSet)
                    );
                }
            }
        }
        errorReporter.logTrace("Loaded %d StorPoolDefinitions", list.size());
        return list;
    }

    public StorPoolDefinitionData load(ResultSet resultSet) throws SQLException
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
            UUID uuid = java.util.UUID.fromString(resultSet.getString(SPD_UUID));

            ObjectProtection objProt = getObjectProtection(storPoolName);

            storPoolDefinition = new StorPoolDefinitionData(
                uuid,
                objProt,
                storPoolName,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );
            errorReporter.logTrace("StorPoolDefinition loaded from DB %s", getId(storPoolName));
        }
        else
        {
            errorReporter.logTrace("StorPoolDefinition loaded from cache %s", getId(storPoolName));
        }
        return storPoolDefinition;
    }

    private ObjectProtection getObjectProtection(StorPoolName storPoolName)
        throws SQLException, ImplementationError
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPathSPD(storPoolName),
            false // no need to log a warning, as we would fail then anyways
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
    public void delete(StorPoolDefinitionData storPoolDefinitionData) throws SQLException
    {
        errorReporter.logTrace("Deleting StorPoolDefinition %s", getId(storPoolDefinitionData));
        try (PreparedStatement stmt = getConnection().prepareStatement(SPD_DELETE))
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

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
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
