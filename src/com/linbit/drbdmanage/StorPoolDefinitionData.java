package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.UUID;

import com.linbit.TransactionMgr;
import com.linbit.TransactionObject;
import com.linbit.drbdmanage.dbdrivers.interfaces.StorPoolDefinitionDataDatabaseDriver;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;

public class StorPoolDefinitionData extends BaseTransactionObject implements StorPoolDefinition
{
    private final UUID uuid;
    private final StorPoolName name;
    private final ObjectProtection objProt;

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
        this(accCtx, nameRef, transMgr, UUID.randomUUID());
    }

    /**
     * Constructor used by other Constructor as well as from the DerbyDriver for
     * restoring the UUID
     *
     * @param accCtx
     * @param nameRef
     * @param transMgr
     * @param id
     * @throws AccessDeniedException
     * @throws SQLException
     */
    StorPoolDefinitionData(AccessContext accCtx, StorPoolName nameRef, TransactionMgr transMgr, UUID id)
        throws AccessDeniedException, SQLException
    {
        name = nameRef;
        uuid = id;
        objProt = ObjectProtection.getInstance(
            accCtx,
            transMgr,
            ObjectProtection.buildPathSPD(nameRef),
            true
        );

        transObjs = Arrays.<TransactionObject>asList(objProt);
    }

    public static StorPoolDefinitionData getInstance(
        AccessContext accCtx,
        StorPoolName nameRef,
        TransactionMgr transMgr,
        boolean createIfNotExists
    )
        throws AccessDeniedException, SQLException
    {
        StorPoolDefinitionData storPoolDfn = null;

        StorPoolDefinitionDataDatabaseDriver dbDriver = DrbdManage.getStorPoolDefinitionDataDriver(nameRef);
        if (transMgr != null)
        {
            storPoolDfn = dbDriver.load(transMgr.dbCon, accCtx, transMgr);
            if (storPoolDfn == null && createIfNotExists)
            {
                storPoolDfn = new StorPoolDefinitionData(accCtx, nameRef, transMgr);
                dbDriver.create(transMgr.dbCon, storPoolDfn);
            }
        }
        else
        if (createIfNotExists)
        {
            storPoolDfn = new StorPoolDefinitionData(accCtx, nameRef, transMgr);
        }

        return storPoolDfn;
    }

    @Override
    public UUID getUuid()
    {
        return uuid;
    }

    @Override
    public ObjectProtection getObjProt()
    {
        return objProt;
    }

    @Override
    public StorPoolName getName()
    {
        return name;
    }
}
