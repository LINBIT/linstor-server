package com.linbit.linstor.layer.luks;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerSizeCalculator;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.kinds.ExtToolsInfo.Version;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class LuksLayerSizeCalculator extends AbsLayerSizeCalculator<LuksVlmData<?>>
{

    // linstor calculates in KiB
    private static final int MIB = 1024;
    private static final int LUKS1_HEADER_SIZE = 2 * MIB;
    private static final int LUKS2_HEADER_SIZE = 16 * MIB;

    @Inject
    public LuksLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.LUKS);
    }

    @Override
    protected void updateAllocatedSizeFromUsableSizeImpl(LuksVlmData<?> luksDataRef)
        throws AccessDeniedException, DatabaseException
    {
        long luksHeaderSize = getLuksHeaderSize(luksDataRef);
        long grossSize = luksDataRef.getUsableSize() + luksHeaderSize;
        luksDataRef.setAllocatedSize(grossSize);

        VlmProviderObject<?> childVlmData = luksDataRef.getSingleChild();
        childVlmData.setUsableSize(grossSize);
        updateAllocatedSizeFromUsableSize(childVlmData);

        /*
         * Layers below us will update our dataChild's usable size.
         * We need to take that updated size for further calculations.
         */
        long usableSizeChild = childVlmData.getUsableSize();
        luksDataRef.setAllocatedSize(usableSizeChild);
        luksDataRef.setUsableSize(usableSizeChild - luksHeaderSize);
    }

    @Override
    protected void updateUsableSizeFromAllocatedSizeImpl(LuksVlmData<?> luksDataRef)
        throws AccessDeniedException, DatabaseException
    {
        long luksHeaderSize = getLuksHeaderSize(luksDataRef);
        long grossSize = luksDataRef.getAllocatedSize();
        long netSize = grossSize - luksHeaderSize;

        luksDataRef.setUsableSize(netSize);

        VlmProviderObject<?> childVlmData = luksDataRef.getSingleChild();
        childVlmData.setAllocatedSize(grossSize);
        updateUsableSizeFromAllocatedSize(childVlmData);

        /*
         * Layers below us will update our dataChild's usable size.
         * We need to take that updated size for further calculations.
         */
        long usableSizeChild = childVlmData.getUsableSize();
        luksDataRef.setAllocatedSize(usableSizeChild);
        luksDataRef.setUsableSize(usableSizeChild - luksHeaderSize);
    }

    private long getLuksHeaderSize(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException
    {
        ExtToolsInfo cryptSetupInfo = vlmDataRef.getRscLayerObject()
            .getAbsResource()
            .getNode()
            .getPeer(sysCtx)
            .getExtToolsManager()
            .getExtToolInfo(ExtTools.CRYPT_SETUP);
        long luksHeaderSize;
        if (cryptSetupInfo != null && cryptSetupInfo.isSupported())
        {
            if (cryptSetupInfo.hasVersionOrHigher(new Version(2, 1)))
            {
                luksHeaderSize = LUKS2_HEADER_SIZE;
            }
            else
            {
                luksHeaderSize = LUKS1_HEADER_SIZE;
            }
        }
        else
        {
            luksHeaderSize = -1;
        }
        return luksHeaderSize;
    }
}
