package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.LinstorRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.LinstorRemoteDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class LinstorRemote extends AbsRemote
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final UUID objId;
    private final LinstorRemoteDatabaseDriver driver;
    private final TransactionSimpleObject<LinstorRemote, URL> url;
    private final TransactionSimpleObject<LinstorRemote, byte[]> encryptedRemotePassphrase;
    private final TransactionSimpleObject<LinstorRemote, UUID> clusterId;
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
        UUID clusterIdRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(objIdRef, transObjFactory, transMgrProvider, objProtRef, remoteNameRef);
        objId = objIdRef;
        driver = driverRef;

        url = transObjFactory.createTransactionSimpleObject(this, urlRef, driver.getUrlDriver());
        encryptedRemotePassphrase = transObjFactory.createTransactionSimpleObject(
            this,
            encryptedTargetPassphraseRef,
            driver.getEncryptedRemotePassphraseDriver()
        );
        clusterId = transObjFactory.createTransactionSimpleObject(this, clusterIdRef, driver.getClusterIdDriver());

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
            encryptedRemotePassphrase,
            clusterId,
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
    public int compareTo(AbsRemote remote)
    {
        int cmp = remote.getClass().getSimpleName().compareTo(LinstorRemote.class.getSimpleName());
        if (cmp == 0)
        {
            cmp = remoteName.compareTo(remote.getName());
        }
        return cmp;
    }


    @Override
    public int hashCode()
    {
        checkDeleted();
        return Objects.hash(remoteName);
    }

    @Override
    public boolean equals(Object obj)
    {
        checkDeleted();
        boolean ret = false;
        if (this == obj)
        {
            ret = true;
        }
        else if (obj instanceof LinstorRemote)
        {
            LinstorRemote other = (LinstorRemote) obj;
            other.checkDeleted();
            ret = Objects.equals(remoteName, other.remoteName);
        }
        return ret;
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

    public @Nullable byte[] getEncryptedRemotePassphrase(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return encryptedRemotePassphrase.get();
    }

    public void setEncryptedRemotePassphase(AccessContext accCtx, byte[] encryptedRemotePassphraseRef)
        throws DatabaseException, AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        encryptedRemotePassphrase.set(encryptedRemotePassphraseRef);
    }

    public void setClusterId(AccessContext accCtx, UUID clusterIdRef) throws DatabaseException, AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        clusterId.set(clusterIdRef);
    }

    public @Nullable UUID getClusterId(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return clusterId.get();
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
        return RemoteType.LINSTOR;
    }


    public LinstorRemotePojo getApiData(AccessContext accCtx, @Nullable Long fullSyncId, @Nullable Long updateId)
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

    @Override
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

    @Override
    protected void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted LinstorRemote");
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "LinstorRemote '" + remoteName.displayValue + "'";
    }
}
