package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.StltRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.stateflags.StateFlagsPersistence;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.inject.Provider;

import java.util.Arrays;
import java.util.UUID;

/**
 * Temporary object storing ip+port other other informations of the target satellite.
 * This object will NOT be persisted.
 * This object is expected to be deleted after a backup shipping.
 */
public class StltRemote extends BaseTransactionObject
    implements Remote, DbgInstanceUuid, Comparable<StltRemote>, ProtectedObject
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final ObjectProtection objProt;
    private final UUID objId;
    private final transient UUID dbgInstanceId;
    private final RemoteName remoteName;
    private final TransactionSimpleObject<StltRemote, String> ip;
    private final TransactionSimpleObject<StltRemote, Integer> port;
    private final TransactionSimpleObject<StltRemote, Boolean> useZstd;
    private final TransactionSimpleObject<StltRemote, Boolean> deleted;
    private final StateFlags<Flags> flags;

    public StltRemote(
        ObjectProtection objProtRef,
        UUID objIdRef,
        RemoteName remoteNameRef,
        long initialFlags,
        String ipRef,
        Integer portRef,
        Boolean useZstdRef,
        StateFlagsPersistence<StltRemote> stateFlagsDriverRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        objProt = objProtRef;
        objId = objIdRef;
        dbgInstanceId = UUID.randomUUID();
        remoteName = remoteNameRef;

        ip = transObjFactory.createTransactionSimpleObject(this, ipRef, null);
        port = transObjFactory.createTransactionSimpleObject(this, portRef, null);
        useZstd = transObjFactory.createTransactionSimpleObject(this, useZstdRef, null);

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            Flags.class,
            stateFlagsDriverRef,
            initialFlags
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            objProt,
            ip,
            port,
            useZstd,
            flags,
            deleted
        );
    }

    @Override
    public ObjectProtection getObjProt()
    {
        checkDeleted();
        return objProt;
    }

    @Override
    public int compareTo(@Nonnull StltRemote s3remote)
    {
        return remoteName.compareTo(s3remote.getName());
    }

    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }

    @Override
    public RemoteName getName()
    {
        checkDeleted();
        return remoteName;
    }

    public String getIp(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return ip.get();
    }

    public void setIp(AccessContext accCtx, String ipRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        ip.set(ipRef);
    }

    public Integer getPort(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return port.get();
    }

    public void setPort(AccessContext accCtx, int portRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        port.set(portRef);
    }

    public Boolean useZstd(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return useZstd.get();
    }

    public void useZstd(AccessContext accCtx, Boolean useZstdRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        useZstd.set(useZstdRef);
    }

    @Override
    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    @Override
    public RemoteType getType()
    {
        return RemoteType.SATELLTE;
    }

    public StltRemotePojo getApiData(AccessContext accCtx, Long fullSyncId, Long updateId) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new StltRemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(accCtx),
            ip.get(),
            port.get(),
            useZstd.get(),
            fullSyncId,
            updateId
        );
    }

    public void applyApiData(AccessContext accCtx, StltRemotePojo apiData)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        ip.set(apiData.getIp());
        port.set(apiData.getPort());
        useZstd.set(apiData.useZstd());

        flags.resetFlagsTo(accCtx, Flags.restoreFlags(apiData.getFlags()));
    }

    public boolean isDeleted()
    {
        return deleted.get();
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            objProt.delete(accCtx);

            activateTransMgr();

            deleted.set(true);
        }
    }

    private void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted S3Remote");
        }
    }

    @Override
    public UUID debugGetVolatileUuid()
    {
        return dbgInstanceId;
    }
}
