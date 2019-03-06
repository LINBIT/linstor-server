package com.linbit.linstor.storage.data.adapter.cryptsetup;

import com.linbit.linstor.Volume;
import com.linbit.linstor.api.pojo.CryptSetupRscPojo.CryptVlmPojo;
import com.linbit.linstor.dbdrivers.interfaces.CryptSetupLayerDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.cryptsetup.CryptSetupVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CryptSetupVlmData extends BaseTransactionObject implements CryptSetupVlmObject
{
    // unmodifiable data, once initialized
    private final Volume vlm;
    private final RscLayerObject rscData;

    // persisted, serialized, ctrl and stlt
    private final TransactionSimpleObject<CryptSetupVlmData, byte[]> encryptedPassword;

    // not persisted, serialized, ctrl and stlt
    private long allocatedSize;
    private long usableSize;
    private @Nullable String devicePath;
    private String backingDevice;
    private String diskState;

    // not persisted, not serialized, stlt only
    private boolean exists;
    private boolean failed;
    private boolean opened;
    private String identifier;
    private byte[] decryptedPassword = null;
    private List<? extends State> unmodStates;
    private Size sizeState;

    // TODO maybe introduce States like "OPEN", "CLOSED", "UNINITIALIZED" or something...

    public CryptSetupVlmData(
        Volume vlmRef,
        CryptSetupRscData rscDataRef,
        byte[] encryptedPasswordRef,
        CryptSetupLayerDatabaseDriver dbDriver,
        TransactionObjectFactory transObjFactory,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);

        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);

        encryptedPassword = transObjFactory.createTransactionSimpleObject(
            this,
            encryptedPasswordRef,
            dbDriver.getVlmEncryptedPasswordDriver()
        );

        transObjs = Arrays.asList(
            vlm,
            rscData,
            encryptedPassword
        );
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    public void setExists(boolean existsRef)
    {
        exists = existsRef;
    }

    public boolean isOpened()
    {
        return opened;
    }

    public void setOpened(boolean openedRef)
    {
        opened = openedRef;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    public void setFailed(boolean failedRef)
    {
        failed = failedRef;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    public void setAllocatedSize(long allocatedSizeRef)
    {
        allocatedSize = allocatedSizeRef;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
    }

    public void setUsableSize(long usableSizeRef)
    {
        usableSize = usableSizeRef;
    }

    @Override
    public @Nullable String getDevicePath()
    {
        return devicePath;
    }

    public void setDevicePath(String devicePathRef)
    {
        devicePath = devicePathRef;
    }

    @Override
    public String getBackingDevice()
    {
        return backingDevice;
    }

    public void setBackingDevice(String backingDeviceRef)
    {
        backingDevice = backingDeviceRef;
    }

    @Override
    public Size getSizeState()
    {
        return sizeState;
    }

    public void setSizeState(Size sizeStateRef)
    {
        sizeState = sizeStateRef;
    }

    public String getDiskState()
    {
        return diskState;
    }

    public void setDiskState(String diskStateRef)
    {
        diskState = diskStateRef;
    }

    @Override
    public List<? extends State> getStates()
    {
        return unmodStates;
    }

    @Override
    public DeviceLayerKind getLayerKind()
    {
        return DeviceLayerKind.CRYPT_SETUP;
    }

    @Override
    public byte[] getEncryptedPassword()
    {
        return encryptedPassword.get();
    }

    @Override
    public Volume getVolume()
    {
        return vlm;
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    @Override
    public RscLayerObject getRscLayerObject()
    {
        return rscData;
    }

    public byte[] getDecryptedPassword()
    {
        return decryptedPassword;
    }

    public void setDecryptedPassword(byte[] decryptedPasswordRef)
    {
        decryptedPassword = decryptedPasswordRef;
    }

    @Override
    public String getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(String identifierRef)
    {
        identifier = identifierRef;
    }

    @Override
    public CryptVlmPojo asPojo(AccessContext accCtxRef)
    {
        return new CryptVlmPojo(
            getVlmNr().value,
            encryptedPassword.get(),
            devicePath,
            backingDevice,
            allocatedSize,
            usableSize,
            opened,
            diskState
        );
    }
}
