package com.linbit.linstor.core.objects.remotes;

import com.linbit.linstor.AccessToDeletedDataException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.S3RemotePojo;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.remotes.S3RemoteDatabaseDriver;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import jakarta.inject.Provider;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class S3Remote extends AbsRemote
{
    public interface InitMaps
    {
        // currently only a place holder for future maps
    }

    private final S3RemoteDatabaseDriver driver;
    private final TransactionSimpleObject<S3Remote, String> endpoint;
    private final TransactionSimpleObject<S3Remote, String> bucket;
    private final TransactionSimpleObject<S3Remote, String> region;
    private final TransactionSimpleObject<S3Remote, byte[]> accessKey;
    private final TransactionSimpleObject<S3Remote, byte[]> secretKey;
    private final StateFlags<Flags> flags;

    // it would be nicer if both booleans could be TransactionSimpleObjects
    // but the stlt changes these as well, and would require scopes in the corresponding threads only for this
    // which is rather cumbersome
    private boolean requesterPaysSupported = true;
    private boolean multiDeleteSupported = true;

    public S3Remote(
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
        super(objIdRef, transObjFactory, transMgrProvider, remoteNameRef);
        driver = driverRef;

        endpoint = transObjFactory.createTransactionSimpleObject(this, endpointRef, driver.getEndpointDriver());
        bucket = transObjFactory.createTransactionSimpleObject(this, bucketRef, driver.getBucketDriver());
        region = transObjFactory.createTransactionSimpleObject(this, regionRef, driver.getRegionDriver());
        accessKey = transObjFactory.createTransactionSimpleObject(this, accessKeyRef, driver.getAccessKeyDriver());
        secretKey = transObjFactory.createTransactionSimpleObject(this, secretKeyRef, driver.getSecretKeyDriver());

        flags = transObjFactory
            .createStateFlagsImpl(this, Flags.class, driver.getStateFlagsPersistence(), initialFlags);

        transObjs = Arrays.asList(
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
    public int compareTo(AbsRemote remote)
    {
        int cmp = remote.getClass().getSimpleName().compareTo(S3Remote.class.getSimpleName());
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
        else if (obj instanceof S3Remote other)
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

    public String getUrl()
    {
        checkDeleted();
        return endpoint.get();
    }

    public void setUrl(String url) throws DatabaseException
    {
        checkDeleted();
        endpoint.set(url);
    }

    public String getBucket()
    {
        checkDeleted();
        return bucket.get();
    }

    public void setBucket(String bucketRef) throws DatabaseException
    {
        checkDeleted();
        bucket.set(bucketRef);
    }

    public String getRegion()
    {
        checkDeleted();
        return region.get();
    }

    public void setRegion(String regionRef) throws DatabaseException
    {
        checkDeleted();
        region.set(regionRef);
    }

    public byte[] getAccessKey()
    {
        checkDeleted();
        return accessKey.get();
    }

    public void setAccessKey(byte[] accessKeyRef) throws DatabaseException
    {
        checkDeleted();
        accessKey.set(accessKeyRef);
    }

    public byte[] getSecretKey()
    {
        checkDeleted();
        return secretKey.get();
    }

    public void setSecretKey(byte[] secretKeyRef) throws DatabaseException
    {
        checkDeleted();
        secretKey.set(secretKeyRef);
    }

    public boolean isRequesterPaysSupported()
    {
        checkDeleted();
        return requesterPaysSupported;
    }

    public void setRequesterPaysSupported(boolean requesterPaysSupportedRef)
    {
        checkDeleted();
        requesterPaysSupported = requesterPaysSupportedRef;
    }

    public boolean isMultiDeleteSupported()
    {
        checkDeleted();
        return multiDeleteSupported;
    }

    public void setMultiDeleteSupported(boolean multiDeleteSupportedRef)
    {
        checkDeleted();
        multiDeleteSupported = multiDeleteSupportedRef;
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
        return RemoteType.S3;
    }

    public S3RemotePojo getApiData(@Nullable Long fullSyncId, @Nullable Long updateId)
    {
        checkDeleted();
        return new S3RemotePojo(
            objId,
            remoteName.displayValue,
            flags.getFlagsBits(),
            endpoint.get(),
            bucket.get(),
            region.get(),
            accessKey.get(),
            secretKey.get(),
            fullSyncId,
            updateId
        );
    }

    public void applyApiData(S3RemotePojo apiData) throws DatabaseException
    {
        checkDeleted();
        endpoint.set(apiData.getEndpoint());
        bucket.set(apiData.getBucket());
        region.set(apiData.getRegion());
        accessKey.set(apiData.getAccessKey());
        secretKey.set(apiData.getSecretKey());

        flags.resetFlagsTo(Flags.restoreFlags(apiData.getFlags()));
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
            throw new AccessToDeletedDataException("Access to deleted S3Remote");
        }
    }

    @Override
    protected String toStringImpl()
    {
        return "S3Remote '" + remoteName.displayValue + "'";
    }
}
