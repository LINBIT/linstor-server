package com.linbit.linstor;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.TransactionMap;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.propscon.PropsContainer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;

public class StorPoolDefinitionData extends BaseTransactionObject implements StorPoolDefinition
{
    private final UUID uuid;
    private final StorPoolName name;
    private final ObjectProtection objProt;
    private final StorPoolDefinitionDataDatabaseDriver dbDriver;
    private final TransactionMap<NodeName, StorPool> storPools;
    private final Props props;

    private boolean deleted = false;

    /**
     * Constructor used by {@link StorPoolDefinition#getInstance(AccessContext, StorPoolName, TransactionMgr, boolean)}
     *
     * @param accCtx
     * @param nameRef
     * @param transMgr
     * @throws AccessDeniedException
     * @throws SQLException
     */
    StorPoolDefinitionData(AccessContext accCtx, StorPoolName nameRef, TransactionMgr transMgr)
        throws AccessDeniedException, SQLException
    {
        this(
            UUID.randomUUID(),
            ObjectProtection.getInstance(
                accCtx,
                ObjectProtection.buildPathSPD(nameRef),
                true,
                transMgr
            ),
            nameRef
        );
    }

    /**
     * Constructor used by other Constructor as well as from the DerbyDriver for
     * restoring the UUID and the ObjectProtection
     *
     * @param accCtx
     * @param nameRef
     * @param transMgr
     * @param id
     * @throws AccessDeniedException
     * @throws SQLException
     */
    StorPoolDefinitionData(UUID id, ObjectProtection objProtRef, StorPoolName nameRef)
        throws SQLException
    {
        uuid = id;
        objProt = objProtRef;
        name = nameRef;
        storPools = new TransactionMap<>(new TreeMap<NodeName, StorPool>(), null);

        props = PropsContainer.getInstance(
            PropsContainer.buildPath(nameRef),
            transMgr
        );

        dbDriver = LinStor.getStorPoolDefinitionDataDatabaseDriver();

        transObjs = Arrays.<TransactionObject>asList(
            objProt,
            storPools,
            props
        );
    }

    public static StorPoolDefinitionData getInstance(
        AccessContext accCtx,
        StorPoolName nameRef,
        TransactionMgr transMgr,
        boolean createIfNotExists,
        boolean failIfExists
    )
        throws AccessDeniedException, SQLException, DrbdDataAlreadyExistsException
    {
        StorPoolDefinitionData storPoolDfn = null;

        StorPoolDefinitionDataDatabaseDriver dbDriver = LinStor.getStorPoolDefinitionDataDatabaseDriver();
        storPoolDfn = dbDriver.load(nameRef, false, transMgr);

        if (failIfExists && storPoolDfn != null)
        {
            throw new DrbdDataAlreadyExistsException("The StorPoolDefinition already exists");
        }

        if (storPoolDfn == null && createIfNotExists)
        {
            storPoolDfn = new StorPoolDefinitionData(accCtx, nameRef, transMgr);

            dbDriver.create(storPoolDfn, transMgr);
        }

        if (storPoolDfn != null)
        {
            storPoolDfn.initialized();
        }

        return storPoolDfn;
    }

    public static StorPoolDefinitionData getInstanceSatellite(
        AccessContext accCtx,
        UUID uuid,
        StorPoolName nameRef,
        SatelliteTransactionMgr transMgr
    )
        throws ImplementationError
    {
        StorPoolDefinitionData storPoolDfn = null;

        StorPoolDefinitionDataDatabaseDriver dbDriver = LinStor.getStorPoolDefinitionDataDatabaseDriver();
        try
        {
            storPoolDfn = dbDriver.load(nameRef, false, transMgr);
            if (storPoolDfn == null)
            {
                storPoolDfn = new StorPoolDefinitionData(
                    uuid,
                    ObjectProtection.getInstance(
                        accCtx,
                        "",
                        true,
                        transMgr
                    ),
                    nameRef
                );
            }
            storPoolDfn.initialized();
        }
        catch (Exception exc)
        {
            throw new ImplementationError(
                "This method should only be called with a satellite db in background!",
                exc
            );
        }
        return storPoolDfn;
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return uuid;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public StorPoolName getName()
    {
        checkDeleted();
        return name;
    }

    @Override
    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return storPools.values().iterator();
    }

    void addStorPool(AccessContext accCtx, StorPoolData storPoolData) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        storPools.put(storPoolData.getNode().getName(), storPoolData);
    }

    void removeStorPool(AccessContext accCtx, StorPoolData storPoolData) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.USE);
        storPools.remove(storPoolData.getNode().getName());
    }

    @Override
    public Props getConfiguration(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, objProt, props);
    }

    @Override
    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CONTROL);

        dbDriver.delete(this, transMgr);
        deleted = true;
    }

    private void checkDeleted()
    {
        if (deleted)
        {
            throw new ImplementationError("Access to deleted node", null);
        }
    }

    @Override
    public String toString()
    {
        return "StorPool: '" + name + "'";
    }
}
