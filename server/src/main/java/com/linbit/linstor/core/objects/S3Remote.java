package com.linbit.linstor.core.objects;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.S3RemoteDatabaseDriver;
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
import javax.inject.Provider;

import java.util.Arrays;
import java.util.UUID;

public class S3Remote extends BaseTransactionObject
    implements Remote, DbgInstanceUuid, Comparable<S3Remote>, ProtectedObject
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final ObjectProtection objProt;
    private final UUID objId;
    private final transient UUID dbgInstanceId;
    private final S3RemoteDatabaseDriver driver;
    private final RemoteName remoteName;
    private final TransactionSimpleObject<S3Remote, String> endpoint;
    private final TransactionSimpleObject<S3Remote, String> bucket;
    private final TransactionSimpleObject<S3Remote, String> region;
    private final TransactionSimpleObject<S3Remote, byte[]> accessKey;
    private final TransactionSimpleObject<S3Remote, byte[]> secretKey;
    private final TransactionSimpleObject<S3Remote, Boolean> deleted;
    private final StateFlags<Flags> flags;

    public S3Remote(
        ObjectProtection objProtRef,
        UUID objIdRef,
        S3RemoteDatabaseDriver driverRef,
        RemoteName remoteNameRef,
        long initialFlags,
        String endpointRef,
        String bucketRef,
        String regionRef,
        byte[] accessKeyRef,
        byte[] secretKeyRef,
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

        endpoint = transObjFactory.createTransactionSimpleObject(this, endpointRef, driver.getEndpointDriver());
        bucket = transObjFactory.createTransactionSimpleObject(this, bucketRef, driver.getBucketDriver());
        region = transObjFactory.createTransactionSimpleObject(this, regionRef, driver.getRegionDriver());
        accessKey = transObjFactory.createTransactionSimpleObject(this, accessKeyRef, driver.getAccessKeyDriver());
        secretKey = transObjFactory.createTransactionSimpleObject(this, secretKeyRef, driver.getSecretKeyDriver());

        flags = transObjFactory
            .createStateFlagsImpl(objProt, this, Flags.class, driver.getStateFlagsPersistence(), initialFlags);

        deleted = transObjFactory.createTransactionSimpleObject(this, false, null);

        transObjs = Arrays.asList(
            objProt,
            endpoint,
            bucket,
            region,
            accessKey,
            secretKey,
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
    public int compareTo(@Nonnull S3Remote s3remote)
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

    public String getUrl(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return endpoint.get();
    }

    public void setUrl(AccessContext accCtx, String url) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        endpoint.set(url);
    }

    public String getBucket(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return bucket.get();
    }

    public void setBucket(AccessContext accCtx, String bucketRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        bucket.set(bucketRef);
    }

    public String getRegion(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return region.get();
    }

    public void setRegion(AccessContext accCtx, String regionRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        region.set(regionRef);
    }

    public byte[] getAccessKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return accessKey.get();
    }

    public void setAccessKey(AccessContext accCtx, byte[] accessKeyRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        accessKey.set(accessKeyRef);
    }

    public byte[] getSecretKey(AccessContext accCtx) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return secretKey.get();
    }

    public void setSecretKey(AccessContext accCtx, byte[] secretKeyRef) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        secretKey.set(secretKeyRef);
    }

    @Override
    public StateFlags<Flags> getFlags()
    {
        checkDeleted();
        return flags;
    }

    public S3RemotePojo getApiData(AccessContext accCtx, Long fullSyncId, Long updateId) throws AccessDeniedException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.VIEW);
        return new S3RemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(accCtx),
            endpoint.get(),
            bucket.get(),
            region.get(),
            accessKey.get(),
            secretKey.get(),
            fullSyncId,
            updateId
        );
    }

    public void applyApiData(AccessContext accCtx, S3RemotePojo apiData) throws AccessDeniedException, DatabaseException
    {
        checkDeleted();
        objProt.requireAccess(accCtx, AccessType.CHANGE);
        endpoint.set(apiData.getEndpoint());
        bucket.set(apiData.getBucket());
        region.set(apiData.getRegion());
        accessKey.set(apiData.getAccessKey());
        secretKey.set(apiData.getSecretKey());
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
