package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
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

public class EbsRemote extends AbsRemote
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final EbsRemoteDatabaseDriver dbDriver;
    private final TransactionSimpleObject<EbsRemote, URL> url;
    private final TransactionSimpleObject<EbsRemote, String> region;
    private final TransactionSimpleObject<EbsRemote, String> availabilityZone;
    private final TransactionSimpleObject<EbsRemote, byte[]> encryptedSecretKey;
    private final TransactionSimpleObject<EbsRemote, String> decryptedSecretKey;
    private final TransactionSimpleObject<EbsRemote, byte[]> encryptedAccessKey;
    private final TransactionSimpleObject<EbsRemote, String> decryptedAccessKey;
    private final StateFlags<Flags> flags;

    public EbsRemote(
        UUID objIdRef,
        EbsRemoteDatabaseDriver dbDriverRef,
        RemoteName remoteNameRef,
        long initialFlags,
        URL urlRef,
        String regionRef,
        String availabilityZoneRef,
        byte[] encryptedSecretKeyRef,
        byte[] encryptedAccessKeyRef,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(objIdRef, transObjFactory, transMgrProvider, remoteNameRef);
        dbDriver = dbDriverRef;
        url = transObjFactory.createTransactionSimpleObject(this, urlRef, dbDriverRef.getUrlDriver());
        availabilityZone = transObjFactory.createTransactionSimpleObject(
            this,
            availabilityZoneRef,
            dbDriverRef.getAvailabilityZoneDriver()
        );
        region = transObjFactory.createTransactionSimpleObject(
            this,
            regionRef,
            dbDriverRef.getRegionDriver()
        );
        encryptedSecretKey = transObjFactory.createTransactionSimpleObject(
            this,
            encryptedSecretKeyRef,
            dbDriverRef.getEncryptedSecretKeyDriver()
        );
        decryptedSecretKey = transObjFactory.createTransactionSimpleObject(this, null, null);

        encryptedAccessKey = transObjFactory.createTransactionSimpleObject(
            this,
            encryptedAccessKeyRef,
            dbDriverRef.getEncryptedAccessKeyDriver()
        );
        decryptedAccessKey = transObjFactory.createTransactionSimpleObject(this, null, null);
        flags = transObjFactory.createStateFlagsImpl(
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initialFlags
        );

        transObjs = Arrays.asList(
            flags,
            url,
            availabilityZone,
            region,
            encryptedAccessKey,
            decryptedAccessKey,
            encryptedSecretKey,
            decryptedSecretKey,
            deleted
        );
    }


    @Override
    public UUID getUuid()
    {
        checkDeleted();
        return objId;
    }


    @Override
    public RemoteType getType()
    {
        checkDeleted();
        return RemoteType.EBS;
    }

    public String getAvailabilityZone()
    {
        checkDeleted();
        return availabilityZone.get();
    }

    public void setAvailabilityZone(String availaibilityZoneRef)
        throws DatabaseException
    {
        checkDeleted();
        availabilityZone.set(availaibilityZoneRef);
    }

    public String getRegion()
    {
        checkDeleted();
        return region.get();
    }

    public void setRegion(String regionRef)
        throws DatabaseException
    {
        checkDeleted();
        region.set(regionRef);
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

    public @Nullable byte[] getEncryptedAccessKey()
    {
        checkDeleted();
        return encryptedAccessKey.get();
    }

    public void setEncryptedAccessKey(byte[] encryptedAccessKeyRef)
        throws DatabaseException
    {
        checkDeleted();
        encryptedAccessKey.set(encryptedAccessKeyRef);
    }

    public String getDecryptedAccessKey()
    {
        checkDeleted();
        return decryptedAccessKey.get();
    }

    public void setDecryptedAccessKey(String decryptedAccessKeyRef)
        throws DatabaseException
    {
        checkDeleted();
        decryptedAccessKey.set(decryptedAccessKeyRef);
    }

    public @Nullable byte[] getEncryptedSecretKey()
    {
        checkDeleted();
        return encryptedSecretKey.get();
    }

    public void setEncryptedSecretKey(byte[] encryptedSecretKeyRef)
        throws DatabaseException
    {
        checkDeleted();
        encryptedSecretKey.set(encryptedSecretKeyRef);
    }

    public String getDecryptedSecretKey()
    {
        checkDeleted();
        return decryptedSecretKey.get();
    }

    public void setDecryptedSecretKey(String decryptedSecretKeyRef)
        throws DatabaseException
    {
        checkDeleted();
        decryptedSecretKey.set(decryptedSecretKeyRef);
    }

    public EbsRemotePojo getApiData(@Nullable Long fullSyncId, @Nullable Long updateId)
    {
        checkDeleted();
        return new EbsRemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(),
            url.get().toString(),
            availabilityZone.get(),
            region.get(),
            encryptedAccessKey.get(),
            encryptedSecretKey.get(),
            fullSyncId,
            updateId
        );
    }
    public void applyApiData(EbsRemotePojo apiData)
        throws DatabaseException, MalformedURLException
    {
        checkDeleted();
        url.set(new URL(apiData.getUrl()));
        availabilityZone.set(apiData.getAvailabilityZone());
        region.set(apiData.getRegion());
        encryptedAccessKey.set(apiData.getAccessKey());
        encryptedSecretKey.set(apiData.getSecretKey());

        flags.resetFlagsTo(Flags.restoreFlags(apiData.getFlags()));
    }

    @Override
    public void delete() throws DatabaseException
    {
        if (!deleted.get())
        {


            activateTransMgr();

            dbDriver.delete(this);

            deleted.set(true);
        }
    }

    @Override
    public int compareTo(AbsRemote remote)
    {
        int cmp = remote.getClass().getSimpleName().compareTo(EbsRemote.class.getSimpleName());
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
        else if (obj instanceof EbsRemote other)
        {
            other.checkDeleted();
            ret = Objects.equals(remoteName, other.remoteName);
        }
        return ret;
    }

    @Override
    protected void checkDeleted()
    {
        if (deleted.get())
        {
            throw new AccessToDeletedDataException("Access to deleted EbsRemote");
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "EbsRemote '" + remoteName.displayValue + "'";
    }

    @Override
    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }
}
