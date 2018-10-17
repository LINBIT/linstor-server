package com.linbit.linstor.storage;

import com.linbit.linstor.Volume;
import java.util.BitSet;

/**
 * Represents the current state of a given volume.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public class VlmStorageState
{
    private static final int EXISTS = 0;

    private static final int SIZE_TOO_LARGE = 1;
    private static final int SIZE_TOO_SMALL = 2;
    private static final int SIZE_WITHIN_TOLERANCE = 3;

    private static final int BLOCKING_UPPER_LAYERS = 4;

    private final BitSet flags;

    /** The actual linstor volume for reference */
    private final Volume vlm;
    private final Long actualSize;
    private final String devicePath;
    private final String identifier;
    private final String volumeGroup;

    public VlmStorageState(
        Volume vlmRef,
        long sizeRef,
        String identifierRef,
        String pathRef,
        String vgStrRef
    )
    {
        vlm = vlmRef;
        identifier = identifierRef;
        actualSize = sizeRef;
        devicePath = pathRef;
        volumeGroup = vgStrRef;

        flags = new BitSet();
    }

    public Volume getVolume()
    {
        return vlm;
    }

    public Long getActualSize()
    {
        return actualSize;
    }

    public String getDevicePath()
    {
        return devicePath;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public String getVolumeGroup()
    {
        return volumeGroup;
    }

    public boolean exists()
    {
        return flags.get(EXISTS);
    }

    public boolean isSizeTooSmall()
    {
        return flags.get(SIZE_TOO_SMALL);
    }

    public boolean isSizeTooLarge()
    {
        return flags.get(SIZE_TOO_LARGE);
    }

    public boolean isSizeWithinTolerance()
    {
        return flags.get(SIZE_WITHIN_TOLERANCE);
    }

    public boolean isBlockingUpperLayers()
    {
        return flags.get(BLOCKING_UPPER_LAYERS);
    }

    public VlmStorageState setExists(boolean existsRef)
    {
        flags.set(EXISTS, existsRef);
        return this;
    }

    public VlmStorageState setSizeTooSmall(boolean sizeTooSmall)
    {
        flags.set(SIZE_TOO_SMALL, sizeTooSmall);
        return this;
    }

    public VlmStorageState setSizeTooLarge(boolean sizeTooLarge)
    {
        flags.set(SIZE_TOO_LARGE, sizeTooLarge);
        return this;
    }

    public VlmStorageState setSizeWithinTolerance(boolean sizeWithinTolerance)
    {
        flags.set(SIZE_WITHIN_TOLERANCE, sizeWithinTolerance);
        return this;
    }

    public VlmStorageState setBlockingUpperLayers(boolean blockingUpperLayers)
    {
        flags.set(BLOCKING_UPPER_LAYERS, blockingUpperLayers);
        return this;
    }
}
