package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public abstract class AbsVolume<RSC extends AbsResource<RSC>>
    extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<AbsVolume<RSC>>, LinstorDataObject
{
    // Object identifier
    private final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the resource this volume belongs to
    protected final RSC absRsc;

    // Properties container for this volume
    protected final Props props;

    protected final TransactionSimpleObject<AbsVolume<RSC>, Boolean> deleted;

    AbsVolume(
        UUID uuid,
        RSC resRef,
        Props propsRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProviderRef
    )
    {
        super(transMgrProviderRef);

        objId = uuid;
        dbgInstanceId = UUID.randomUUID();
        absRsc = resRef;

        props = propsRef;
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = new ArrayList<>(
            Arrays.asList(
                absRsc,
                props,
                deleted
            )
        );
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, absRsc.getObjProt(), props);
    }

    public RSC getAbsResource()
    {
        checkDeleted();
        return absRsc;
    }

    public ResourceDefinition getResourceDefinition()
    {
        checkDeleted();
        return absRsc.getResourceDefinition();
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    protected void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted volume");
        }
    }

    public abstract VolumeDefinition getVolumeDefinition();

    public abstract void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    public abstract VolumeNumber getVolumeNumber();

    public abstract long getVolumeSize(AccessContext dbCtxRef) throws AccessDeniedException;
}
