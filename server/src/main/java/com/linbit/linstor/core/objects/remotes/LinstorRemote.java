package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.LinstorRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.LinstorRemoteDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

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

    private final LinstorRemoteDatabaseDriver driver;
    private final TransactionSimpleObject<LinstorRemote, URL> url;
    private final TransactionSimpleObject<LinstorRemote, byte[]> encryptedRemotePassphrase;
    private final TransactionSimpleObject<LinstorRemote, UUID> clusterId;
    private final StateFlags<Flags> flags;

    public LinstorRemote(
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
        super(objIdRef, transObjFactory, transMgrProvider, remoteNameRef);
        driver = driverRef;

        url = transObjFactory.createTransactionSimpleObject(this, urlRef, driver.getUrlDriver());
        encryptedRemotePassphrase = transObjFactory.createTransactionSimpleObject(
            this,
            encryptedTargetPassphraseRef,
            driver.getEncryptedRemotePassphraseDriver()
        );
        clusterId = transObjFactory.createTransactionSimpleObject(this, clusterIdRef, driver.getClusterIdDriver());

        flags = transObjFactory.createStateFlagsImpl(
            this,
            Flags.class,
            driver.getStateFlagsPersistence(),
            initialFlags
        );

        transObjs = Arrays.asList(
            url,
            encryptedRemotePassphrase,
            clusterId,
            deleted
        );
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
        else if (obj instanceof LinstorRemote other)
        {
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

    public URL getUrl()
    {
        checkDeleted();
        return url.get();
    }

    public void setUrl(URL urlRef) throws DatabaseException
    {
        checkDeleted();
        url.set(urlRef);
    }

    public @Nullable byte[] getEncryptedRemotePassphrase()
    {
        checkDeleted();
        return encryptedRemotePassphrase.get();
    }

    public void setEncryptedRemotePassphase(byte[] encryptedRemotePassphraseRef)
        throws DatabaseException
    {
        checkDeleted();
        encryptedRemotePassphrase.set(encryptedRemotePassphraseRef);
    }

    public void setClusterId(UUID clusterIdRef) throws DatabaseException
    {
        checkDeleted();
        clusterId.set(clusterIdRef);
    }

    public @Nullable UUID getClusterId()
    {
        checkDeleted();
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


    public LinstorRemotePojo getApiData(@Nullable Long fullSyncId, @Nullable Long updateId)
    {
        checkDeleted();
        return new LinstorRemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(),
            url.get().toString(),
            fullSyncId,
            updateId
        );
    }

    public void applyApiData(LinstorRemotePojo apiData)
        throws DatabaseException, MalformedURLException
    {
        checkDeleted();
        url.set(new URL(apiData.getUrl()));
    }

    @Override
    public boolean isDeleted()
    {
        return deleted.get();
    }

    @Override
    public void delete() throws DatabaseException
    {
        if (!deleted.get())
        {


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
