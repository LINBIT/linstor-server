package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.EbsRemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.EbsRemoteDatabaseDriver;
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
        ObjectProtection objProtRef,
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
        super(objIdRef, transObjFactory, transMgrProvider, objProtRef, remoteNameRef);
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
            objProtRef,
            this,
            Flags.class,
            dbDriverRef.getStateFlagsPersistence(),
            initialFlags
        );

        transObjs = Arrays.asList(
            objProt,
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

    public String getAvailabilityZone(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return availabilityZone.get();
    }

    public void setAvailabilityZone(AccessContext accCtx, String availaibilityZoneRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        availabilityZone.set(availaibilityZoneRef);
    }

    public String getRegion(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return region.get();
    }

    public void setRegion(AccessContext accCtx, String regionRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        region.set(regionRef);
    }

    public URL getUrl(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return url.get();
    }

    public void setUrl(AccessContext accCtx, URL urlRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        url.set(urlRef);
    }

    public @Nullable byte[] getEncryptedAccessKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return encryptedAccessKey.get();
    }

    public void setEncryptedAccessKey(AccessContext accCtx, byte[] encryptedAccessKeyRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        encryptedAccessKey.set(encryptedAccessKeyRef);
    }

    public String getDecryptedAccessKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return decryptedAccessKey.get();
    }

    public void setDecryptedAccessKey(AccessContext accCtx, String decryptedAccessKeyRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        decryptedAccessKey.set(decryptedAccessKeyRef);
    }

    public @Nullable byte[] getEncryptedSecretKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return encryptedSecretKey.get();
    }

    public void setEncryptedSecretKey(AccessContext accCtx, byte[] encryptedSecretKeyRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        encryptedSecretKey.set(encryptedSecretKeyRef);
    }

    public String getDecryptedSecretKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return decryptedSecretKey.get();
    }

    public void setDecryptedSecretKey(AccessContext accCtx, String decryptedSecretKeyRef)
        throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        decryptedSecretKey.set(decryptedSecretKeyRef);
    }

    public EbsRemotePojo getApiData(AccessContext accCtx, @Nullable Long fullSyncId, @Nullable Long updateId)
        throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new EbsRemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(accCtx),
            url.get().toString(),
            availabilityZone.get(),
            region.get(),
            encryptedAccessKey.get(),
            encryptedSecretKey.get(),
            fullSyncId,
            updateId
        );
    }
    public void applyApiData(AccessContext accCtx, EbsRemotePojo apiData)
        throws AccessDeniedException, DatabaseException, MalformedURLException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        url.set(new URL(apiData.getUrl()));
        availabilityZone.set(apiData.getAvailabilityZone());
        region.set(apiData.getRegion());
        encryptedAccessKey.set(apiData.getAccessKey());
        encryptedSecretKey.set(apiData.getSecretKey());

        flags.resetFlagsTo(accCtx, Flags.restoreFlags(apiData.getFlags()));
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException
    {
        if (!deleted.get())
        {
            objProt.requireAccess(accCtx, AccessType.CONTROL);

            objProt.delete(accCtx);

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
        else if (obj instanceof EbsRemote)
        {
            EbsRemote other = (EbsRemote) obj;
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
