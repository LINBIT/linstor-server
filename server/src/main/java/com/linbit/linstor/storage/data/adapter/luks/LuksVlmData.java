package com.linbit.linstor.storage.data.adapter.luks;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.pojo.LuksRscPojo.LuksVlmPojo;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.dbdrivers.interfaces.LayerLuksVlmDatabaseDriver;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.data.AbsVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmDfnLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmLayerObject;
import com.linbit.linstor.storage.interfaces.layers.State;
import com.linbit.linstor.storage.interfaces.layers.luks.LuksVlmObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObjectFactory;
import com.linbit.linstor.transaction.TransactionSimpleObject;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LuksVlmData<RSC extends AbsResource<RSC>>
    extends AbsVlmData<RSC, LuksRscData<RSC>>
    implements LuksVlmObject<RSC>, VlmLayerObject<RSC>
{
    // persisted, serialized, ctrl and stlt
    private final TransactionSimpleObject<LuksVlmData<?>, byte[]> encryptedPassword;

    private @Nullable String dataDevice;
    private @Nullable String diskState;

    // not persisted, not serialized, stlt only
    private boolean opened;
    private @Nullable String identifier;
    private @Nullable byte[] decryptedPassword = null;
    /**
     * This field will be set, if we want to change the luks password on the satellite.
     */
    private @Nullable byte[] modifyPassword = null;
    private final List<? extends State> unmodStates;
    private @Nullable Size sizeState;

    // TODO maybe introduce States like "OPEN", "CLOSED", "UNINITIALIZED" or something...

    public LuksVlmData(
        AbsVolume<RSC> vlmRef,
        LuksRscData<RSC> rscDataRef,
        byte[] encryptedPasswordRef,
        LayerLuksVlmDatabaseDriver luksVlmDbDriver,
        TransactionObjectFactory transObjFactory,
        Provider<? extends TransactionMgr> transMgrProvider
    )
    {
        super(vlmRef, rscDataRef, transObjFactory, transMgrProvider);

        unmodStates = Collections.emptyList();

        encryptedPassword = transObjFactory.createTransactionSimpleObject(
            this,
            encryptedPasswordRef,
            luksVlmDbDriver.getVlmEncryptedPasswordDriver()
        );

        transObjs = Arrays.asList(
            vlm,
            rscData,
            encryptedPassword
        );
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
    public long getUsableSize()
    {
        return usableSize.get();
    }

    @Override
    public long getOriginalSize()
    {
        return originalSize;
    }

    @Override
    public void setOriginalSize(long originalSizeRef)
    {
        originalSize = originalSizeRef;
    }

    @Override
    public void setUsableSize(long usableSizeRef) throws DatabaseException
    {
        if (usableSizeRef != usableSize.get())
        {
            if (usableSize.get() < usableSizeRef)
            {
                sizeState = Size.TOO_SMALL;
            }
            else
            {
                sizeState = Size.TOO_LARGE;
            }
        }
        else
        {
            sizeState = Size.AS_EXPECTED;
        }
        usableSize.set(usableSizeRef);
    }

    @Override
    public @Nullable String getDataDevice()
    {
        return dataDevice;
    }

    public void setDataDevice(String dataDeviceRef)
    {
        dataDevice = dataDeviceRef;
    }

    @Override
    public @Nullable Size getSizeState()
    {
        return sizeState;
    }

    public void setSizeState(Size sizeStateRef)
    {
        sizeState = sizeStateRef;
    }

    public @Nullable String getDiskState()
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
        return DeviceLayerKind.LUKS;
    }

    @Override
    public byte[] getEncryptedKey()
    {
        return encryptedPassword.get();
    }

    public void setEncryptedKey(byte[] encryptedKeyRef) throws DatabaseException
    {
        encryptedPassword.set(encryptedKeyRef);
    }

    @Override
    public @Nullable VlmDfnLayerObject getVlmDfnLayerObject()
    {
        return null;
    }

    public @Nullable byte[] getDecryptedPassword()
    {
        return decryptedPassword;
    }

    public void setDecryptedPassword(byte[] decryptedPasswordRef)
    {
        decryptedPassword = decryptedPasswordRef;
    }

    public @Nullable byte[] getModifyPassword()
    {
        return modifyPassword;
    }

    public void setModifyPassword(@Nullable byte[] modifyPasswordRef)
    {
        modifyPassword = modifyPasswordRef;
    }

    @Override
    public @Nullable String getIdentifier()
    {
        return identifier;
    }

    public void setIdentifier(String identifierRef)
    {
        identifier = identifierRef;
    }

    @Override
    public LuksVlmPojo asPojo(AccessContext accCtxRef)
    {
        return new LuksVlmPojo(
            getVlmNr().value,
            encryptedPassword.get(),
            devicePath.get(),
            dataDevice,
            allocatedSize.get(),
            usableSize.get(),
            opened,
            diskState,
            discGran.get(),
            exists.get(),
            modifyPassword
        );
    }
}
