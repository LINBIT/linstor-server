package com.linbit.linstor.core.objects;

import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.linstor.LinStorDBRuntimeException;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.core.identifier.KeyValueStoreName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.derby.DbConstants;
import com.linbit.linstor.dbdrivers.interfaces.KeyValueStoreDatabaseDriver;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.PropsContainerFactory;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ObjectProtectionDatabaseDriver;
import com.linbit.linstor.transaction.TransactionMgrSQL;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.utils.Pair;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class KeyValueStoreGenericDbDriver implements KeyValueStoreDatabaseDriver
{
    private static final String TBL_KVS = DbConstants.TBL_KEY_VALUE_STORE;

    private static final String KVS_UUID = DbConstants.UUID;
    private static final String KVS_NAME = DbConstants.KVS_NAME;
    private static final String KVS_DSP_NAME = DbConstants.KVS_DSP_NAME;

    private static final String KVS_SELECT_ALL =
        " SELECT " + KVS_UUID + ", " + KVS_NAME + ", " + KVS_DSP_NAME +
        " FROM " + TBL_KVS;
    private static final String KVS_SELECT =
        KVS_SELECT_ALL +
        " WHERE " + KVS_NAME + " = ?";

    private static final String KVS_INSERT =
        " INSERT INTO " + TBL_KVS +
        " (" + KVS_UUID + ", " + KVS_NAME + ", " + KVS_DSP_NAME + ")" +
        " VALUES (?, ?, ?)";

    private static final String KVS_DELETE =
        " DELETE FROM " + TBL_KVS +
        " WHERE " + KVS_NAME + " = ?";

    private final AccessContext dbCtx;
    private final ErrorReporter errorReporter;

    private final ObjectProtectionDatabaseDriver objProtDriver;
    private final PropsContainerFactory propsContainerFactory;
    private final TransactionObjectFactory transObjFactory;
    private final Provider<TransactionMgrSQL> transMgrProvider;

    @Inject
    public KeyValueStoreGenericDbDriver(
        @SystemContext AccessContext accCtx,
        ErrorReporter errorReporterRef,
        ObjectProtectionDatabaseDriver objProtDriverRef,
        PropsContainerFactory propsContainerFactoryRef,
        TransactionObjectFactory transObjFactoryRef,
        Provider<TransactionMgrSQL> transMgrProviderRef
    )
    {
        dbCtx = accCtx;
        errorReporter = errorReporterRef;
        objProtDriver = objProtDriverRef;
        propsContainerFactory = propsContainerFactoryRef;
        transObjFactory = transObjFactoryRef;
        transMgrProvider = transMgrProviderRef;
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void create(KeyValueStore kvs) throws DatabaseException
    {
        errorReporter.logTrace("Creating KeyValueStore %s", getId(kvs));
        try (PreparedStatement stmt = getConnection().prepareStatement(KVS_INSERT))
        {
            stmt.setString(1, kvs.getUuid().toString());
            stmt.setString(2, kvs.getName().value);
            stmt.setString(3, kvs.getName().displayValue);
            stmt.executeUpdate();

            errorReporter.logTrace("KeyValueStore created %s", getId(kvs));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
    }

    public Map<KeyValueStore, KeyValueStore.InitMaps> loadAll() throws DatabaseException
    {
        errorReporter.logTrace("Loading all KeyValueStores");
        Map<KeyValueStore, KeyValueStore.InitMaps> kvsMap = new TreeMap<>();
        try (PreparedStatement stmt = getConnection().prepareStatement(KVS_SELECT_ALL))
        {
            try (ResultSet resultSet = stmt.executeQuery())
            {
                while (resultSet.next())
                {
                    Pair<KeyValueStore, KeyValueStore.InitMaps> pair = restoreKvs(resultSet);
                    kvsMap.put(pair.objA, pair.objB);
                }
            }
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("Loaded %d KeyValueStores", kvsMap.size());
        return kvsMap;
    }

    private Pair<KeyValueStore, KeyValueStore.InitMaps> restoreKvs(ResultSet resultSet) throws DatabaseException
    {
        Pair<KeyValueStore, KeyValueStore.InitMaps> retPair = new Pair<>();
        KeyValueStore kvs;
        KeyValueStoreName kvsName;

        try
        {
            try
            {
                kvsName = new KeyValueStoreName(resultSet.getString(KVS_DSP_NAME));
            }
            catch (InvalidNameException invalidNameExc)
            {
                throw new LinStorDBRuntimeException(
                    String.format(
                        "The display name of a stored KeyValueStore in the table %s could not be restored. " +
                        "(invalid display KvsName=%s)",
                        TBL_KVS,
                        resultSet.getString(KVS_DSP_NAME)
                    ),
                    invalidNameExc
                );
            }

            ObjectProtection objProt = getObjectProtection(kvsName);

            kvs = new KeyValueStore(
                java.util.UUID.fromString(resultSet.getString(KVS_UUID)),
                objProt,
                kvsName,
                this,
                propsContainerFactory,
                transObjFactory,
                transMgrProvider
            );

            retPair.objA = kvs;
            retPair.objB = new KvsInitMaps();

            errorReporter.logTrace("KeyValueStore instance created %s", getId(kvs));
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        return retPair;
    }

    private ObjectProtection getObjectProtection(KeyValueStoreName kvsName)
        throws DatabaseException, ImplementationError
    {
        ObjectProtection objProt = objProtDriver.loadObjectProtection(
            ObjectProtection.buildPath(kvsName),
            false // no need to log a warning, as we would fail then anyways
        );
        if (objProt == null)
        {
            throw new ImplementationError(
                "KeyValueStore's DB entry exists, but is missing an entry in ObjProt table! " +
                getId(kvsName), null
            );
        }
        return objProt;
    }

    @Override
    public void delete(KeyValueStore kvs) throws DatabaseException
    {
        errorReporter.logTrace("Deleting KeyValueStore %s", getId(kvs));
        try (PreparedStatement stmt = getConnection().prepareStatement(KVS_DELETE))
        {
            stmt.setString(1, kvs.getName().value);
            stmt.executeUpdate();
        }
        catch (SQLException sqlExc)
        {
            throw new DatabaseException(sqlExc);
        }
        errorReporter.logTrace("KeyValueStore deleted %s", getId(kvs));
    }

    private Connection getConnection()
    {
        return transMgrProvider.get().getConnection();
    }

    private String getId(KeyValueStore kvs)
    {
        return getId(kvs.getName().displayValue);
    }

    private String getId(KeyValueStoreName kvsName)
    {
        return getId(kvsName.displayValue);
    }

    private String getId(String kvsName)
    {
        return "(KvsName=" + kvsName + ")";
    }

    private class KvsInitMaps implements KeyValueStore.InitMaps
    {
        // place holder class for future init maps
    }
}
