package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.PropsAccess;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class AbsResource<RSC extends AbsResource<RSC>>
    extends BaseTransactionObject
    implements DbgInstanceUuid, Comparable<AbsResource<RSC>>, ProtectedObject
{
    // Object identifier
    protected final UUID objId;

    // Runtime instance identifier for debug purposes
    private final transient UUID dbgInstanceId;

    // Reference to the node this resource is assigned to
    protected final Node node;

    // Properties container for this resource
    protected final Props props;

    protected final TransactionSimpleObject<AbsResource<RSC>, AbsRscLayerObject<RSC>> rootLayerData;

    protected final TransactionSimpleObject<AbsResource<RSC>, Boolean> deleted;

    public AbsResource(
        UUID objIdRef,
        Node nodeRef,
        Props propsRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactory
    )
    {
        super(transMgrProviderRef);
        ErrorCheck.ctorNotNull(this.getClass(), Node.class, nodeRef);
        objId = objIdRef;
        dbgInstanceId = UUID.randomUUID();
        node = nodeRef;
        props = propsRef;
        rootLayerData = transObjFactory.createTransactionSimpleObject(this, null, null);
        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = new ArrayList<>();
        transObjs.add(node);
        transObjs.add(props);
        transObjs.add(rootLayerData);
        transObjs.add(deleted);
    }

    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        return PropsAccess.secureGetProps(accCtx, getObjProt(), props);
    }

    public Node getNode()
    {
        checkDeleted();
        return node;
    }

    public AbsRscLayerObject<RSC> getLayerData(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.USE);
        return rootLayerData.get();
    }

    public void setLayerData(AccessContext accCtx, AbsRscLayerObject<RSC> layerData)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.USE);
        rootLayerData.set(layerData);
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    protected void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted resource");
        }
    }

    public abstract AbsVolume<RSC> getVolume(VolumeNumber vlmNr);

    public abstract Iterator<? extends AbsVolume<RSC>> iterateVolumes();

    protected abstract Stream<? extends AbsVolume<RSC>> streamVolumes();

    public abstract ResourceDefinition getResourceDefinition();

    public abstract void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    public abstract void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

}
