package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.LinstorRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LinstorRemoteDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.UUID;

public class LinstorRemote extends BaseTransactionObject
    implements Remote, DbgInstanceUuid, Comparable<LinstorRemote>, ProtectedObject
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final ObjectProtection objProt;
    private final UUID objId;
    private final transient UUID dbgInstanceId;
    private final LinstorRemoteDatabaseDriver driver;
    private final RemoteName remoteName;
    private final TransactionSimpleObject<LinstorRemote, URL> url;
    private final TransactionSimpleObject<LinstorRemote, byte[]> encryptedTargetPassphrase;
    private final TransactionSimpleObject<LinstorRemote, Boolean> deleted;
    private final StateFlags<Flags> flags;

    public LinstorRemote(
        ObjectProtection objProtRef,
        UUID objIdRef,
        LinstorRemoteDatabaseDriver driverRef,
        RemoteName remoteNameRef,
        long initialFlags,
        URL urlRef,
        @Nullable byte[] encryptedTargetPassphraseRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);
        objProt = objProtRef;
        objId = objIdRef;
        dbgInstanceId = UUID.randomUUID();
        remoteName = remoteNameRef;
        driver = driverRef;

        url = transObjFactory.createTransactionSimpleObject(this, urlRef, driver.getUrlDriver());
        encryptedTargetPassphrase = transObjFactory.createTransactionSimpleObject(
            this,
            encryptedTargetPassphraseRef,
            driver.getEncryptedPassphraseDriver()
        );

        flags = transObjFactory.createStateFlagsImpl(
            objProt,
            this,
            Flags.class,
            driver.getStateFlagsPersistence(),
            initialFlags
        );

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            objProt,
            url,
            encryptedTargetPassphrase,
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
    public int compareTo(@Nonnull LinstorRemote s3remote)
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

    public URL getUrl(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return url.get();
    }

    public void setUrl(AccessContext accCtx, URL urlRef) throws DatabaseException, AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        url.set(urlRef);
    }


    public byte[] getEncryptedTargetPassphrase(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return encryptedTargetPassphrase.get();
    }

    public void setEncryptedTargetPassphase(AccessContext accCtx, byte[] encryptedTargetPassphraseRef)
        throws DatabaseException, AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        encryptedTargetPassphrase.set(encryptedTargetPassphraseRef);
    }

    @Override
    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public LinstorRemotePojo getApiData(AccessContext accCtx, Long fullSyncId, Long updateId)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new LinstorRemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(accCtx),
            url.get().toString(),
            fullSyncId,
            updateId
        );
    }

    public void applyApiData(AccessContext accCtx, LinstorRemotePojo apiData)
        throws AccessDeniedException, DatabaseException, MalformedURLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        url.set(new URL(apiData.getUrl()));
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

            driver.delete(this);

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
