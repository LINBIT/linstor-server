package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.linbit.ImplementationError;
import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.drbdmanage.core.DrbdManage;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.AccessType;
import com.linbit.drbdmanage.security.ObjectProtection;

public class StorPoolDefinitionData extends BaseTransactionObject implements StorPoolDefinition
{
    private final UUID uuid;
    private final StorPoolName name;
    private final ObjectProtection objProt;
    private final StorPoolDefinitionDataDatabaseDriver dbDriver;
    private final Map<NodeName, StorPool> storPools;

    private boolean deleted = false;

    /**
     * Constructor used by {@link #getInstance(AccessContext, StorPoolName, TransactionMgr, boolean)}
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
    {
        uuid = id;
        objProt = objProtRef;
        name = nameRef;
        storPools = new TreeMap<>();

        dbDriver = DrbdManage.getStorPoolDefinitionDataDatabaseDriver();

        transObjs = Arrays.<TransactionObject>asList(objProt);
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

        StorPoolDefinitionDataDatabaseDriver dbDriver = DrbdManage.getStorPoolDefinitionDataDatabaseDriver();
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

}
