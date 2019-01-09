package com.linbit.linstor.storage.layer.adapter.cryptsetup;

import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.cryptsetup.CryptSetupVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.BaseTransactionObject;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Provider;

import java.util.List;
import java.util.Objects;

public class CryptSetupVlmStltData extends BaseTransactionObject implements CryptSetupVlmObject
{
    final Volume vlm;
    final RscLayerObject rscData;

    boolean exists;
    boolean failed;
    long allocatedSize;
    long usableSize;
    String devicePath;
    String backingDevice;
    List<? extends State> unmodStates;
    Size sizeState;

    byte[] password;

    transient boolean opened;
    transient String identifier;

    // TODO maybe introduce States like "OPEN", "CLOSED", "UNINITIALIZED" or something...

    public CryptSetupVlmStltData(
        Volume vlmRef,
        CryptSetupRscStltData rscDataRef,
        byte[] passwordRef,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(transMgrProvider);

        vlm = Objects.requireNonNull(vlmRef);
        rscData = Objects.requireNonNull(rscDataRef);

        password = passwordRef;
    }

    @Override
    public boolean exists()
    {
        return exists;
    }

    @Override
    public boolean isFailed()
    {
        return failed;
    }

    @Override
    public long getAllocatedSize()
    {
        return allocatedSize;
    }

    @Override
    public long getUsableSize()
    {
        return usableSize;
    }

    @Override
    public String getDevicePath()
    {
        return devicePath;
    }

    @Override
    public String getBackingDevice()
    {
        return backingDevice;
    }

    @Override
    public Size getSizeState()
    {
        return sizeState;
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
    public byte[] getPassword()
    {
        return password;
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
}
