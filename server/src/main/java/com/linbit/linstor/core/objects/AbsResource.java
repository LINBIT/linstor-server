package com.linbit.linstor.core.objects;

import com.linbit.ErrorCheck;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.AbsResourceDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class AbsResource<RSC extends AbsResource<RSC>>
    extends AbsCoreObj<AbsResource<RSC>>
    implements ProtectedObject
{
    // use special epoch time to mark this as a new resource which will get set on resource apply
    // mysql/mariadb do not allow 0 here, so I choose 1000, as it doesn't mather
    public static final int CREATE_DATE_INIT_VALUE = 1000;

    // Reference to the node this resource is assigned to
    protected final Node node;

    protected final TransactionSimpleObject<AbsResource<RSC>, @Nullable Date> createTimestamp;

    protected final TransactionSimpleObject<AbsResource<RSC>, @Nullable AbsRscLayerObject<RSC>> rootLayerData;

    protected AbsResource(
        UUID objIdRef,
        Node nodeRef,
        Provider<? extends TransactionMgr> transMgrProviderRef,
        TransactionObjectFactory transObjFactory,
        @Nullable Date createTimestampRef,
        AbsResourceDatabaseDriver<RSC> dbDriverRef
    )
    {
        super(objIdRef, transObjFactory, transMgrProviderRef);
        ErrorCheck.ctorNotNull(this.getClass(), Node.class, nodeRef);
        node = nodeRef;
        createTimestamp = transObjFactory.createTransactionSimpleObject(
            this,
            createTimestampRef,
            dbDriverRef.getCreateTimeDriver());
        rootLayerData = transObjFactory.createTransactionSimpleObject(this, null, null);

        transObjs = new ArrayList<>();
        transObjs.add(node);
        transObjs.add(rootLayerData);
        transObjs.add(deleted);
        transObjs.add(createTimestamp);
    }


    public Node getNode()
    {
        checkDeleted();
        return node;
    }

    public Optional<Date> getCreateTimestamp()
    {
        checkDeleted();
        return Optional.ofNullable(createTimestamp.get());
    }

    public void setCreateTimestamp(AccessContext accCtx, Date creationDate)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.CHANGE);
        createTimestamp.set(creationDate);
    }

    public @Nullable AbsRscLayerObject<RSC> getLayerData(AccessContext accCtx)
        throws AccessDeniedException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.USE);
        return rootLayerData.get();
    }

    public void setLayerData(AccessContext accCtx, @Nullable AbsRscLayerObject<RSC> layerData)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        getObjProt().requireAccess(accCtx, AccessType.USE);
        rootLayerData.set(layerData);
    }

    public abstract @Nullable AbsVolume<RSC> getVolume(VolumeNumber vlmNr);

    public abstract Iterator<? extends AbsVolume<RSC>> iterateVolumes();

    public abstract Stream<? extends AbsVolume<RSC>> streamVolumes();

    public abstract ResourceDefinition getResourceDefinition();

    public abstract void markDeleted(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

    @Override
    public abstract void delete(AccessContext accCtx)
        throws AccessDeniedException, DatabaseException;

}
