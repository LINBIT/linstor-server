package com.linbit.linstor.layer.drbd;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.AlStripesException;
import com.linbit.drbd.md.MaxAlSizeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinAlSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbd.md.PeerCountException;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerSizeCalculator;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdRscData;
import com.linbit.linstor.storage.data.adapter.drbd.DrbdVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class DrbdLayerSizeCalculator extends AbsLayerSizeCalculator
{
    // Number of activity log stripes for DRBD meta data; this should be replaced with a property of the
    // resource definition, a property of the volume definition, or otherwise a system-wide default
    public static final int FIXME_AL_STRIPES = 1;

    // Number of activity log stripes; this should be replaced with a property of the resource definition,
    // a property of the volume definition, or or otherwise a system-wide default
    public static final long FIXME_AL_STRIPE_SIZE = 32;

    @Inject
    public DrbdLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef)
    {
        super(initRef, DeviceLayerKind.DRBD);
    }

    @Override
    protected void updateAllocatedSizeFromUsableSizeImpl(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        DrbdVlmData<?> drbdVlmData = (DrbdVlmData<?>) vlmDataRef;
        DrbdRscData<?> drbdRscData = drbdVlmData.getRscLayerObject();
        short peerSlots = drbdRscData.getPeerSlots();

        try
        {
            boolean isDiskless = isDiskless(drbdVlmData.getRscLayerObject().getAbsResource());
            if (!isDiskless && !drbdRscData.isSkipDiskEnabled(sysCtx, stltProps))
            {
                long netSize = drbdVlmData.getUsableSize();

                VlmProviderObject<?> dataChild = drbdVlmData.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
                if (drbdVlmData.isUsingExternalMetaData())
                {
                    dataChild.setUsableSize(netSize);
                    updateAllocatedSizeFromUsableSize(dataChild);

                    /*
                     * Layers below us will update our dataChild's usable size.
                     * We need to take that updated size for further calculations.
                     *
                     * The reason for this is that if the external-md size would exactly match the extent size
                     * of the storage, but our data size would not, our data size would need to be rounded
                     * up. But that also needs to be considered for the external-md size as well.
                     */
                    netSize = dataChild.getUsableSize();

                    long extMdSize = new MetaData().getExternalMdSize(
                        netSize,
                        peerSlots,
                        FIXME_AL_STRIPES,
                        FIXME_AL_STRIPE_SIZE
                    );

                    VlmProviderObject<?> metaChild = drbdVlmData
                        .getChildBySuffix(RscLayerSuffixes.SUFFIX_DRBD_META);
                    if (metaChild != null)
                    {
                        // is null if we are nvme-traget while the drbd-ext-metadata stays on the initiator side
                        metaChild.setUsableSize(extMdSize);
                        updateAllocatedSizeFromUsableSize(metaChild);
                    }

                    drbdVlmData.setAllocatedSize(netSize + extMdSize); // rough estimation
                }
                else
                {
                    long grossSize = new MetaData().getGrossSize(
                        netSize,
                        peerSlots,
                        FIXME_AL_STRIPES,
                        FIXME_AL_STRIPE_SIZE
                    );
                    dataChild.setUsableSize(grossSize);
                    updateAllocatedSizeFromUsableSize(dataChild);

                    /*
                     * Layers below us will update our dataChild's usable size.
                     * We need to take that updated size for further calculations.
                     */
                    netSize = new MetaData().getNetSize(
                        dataChild.getUsableSize(),
                        peerSlots,
                        FIXME_AL_STRIPES,
                        FIXME_AL_STRIPE_SIZE
                    );

                    drbdVlmData.setAllocatedSize(grossSize);
                }

                // we need to update the usable size once again since the layers below us
                // might have given us more data than we asked for.
                drbdVlmData.setUsableSize(netSize);
            }
        }
        catch (
            InvalidKeyException | IllegalArgumentException | MinSizeException | MaxSizeException |
            MinAlSizeException | MaxAlSizeException | AlStripesException | PeerCountException exc
        )
        {
            throw new ImplementationError(exc);
        }
    }

    @Override
    protected void updateUsableSizeFromAllocatedSizeImpl(VlmProviderObject<?> vlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        DrbdVlmData<?> drbdVlmData = (DrbdVlmData<?>) vlmDataRef;
        DrbdRscData<?> drbdRscData = drbdVlmData.getRscLayerObject();
        short peerSlots = drbdRscData.getPeerSlots();

        try
        {
            boolean isDiskless = isDiskless(drbdVlmData.getRscLayerObject().getAbsResource());
            if (!isDiskless && !drbdRscData.isSkipDiskEnabled(sysCtx, stltProps))
            {
                // let next layer calculate
                VlmProviderObject<?> dataChildVlmData = drbdVlmData.getChildBySuffix(
                    RscLayerSuffixes.SUFFIX_DATA
                );
                dataChildVlmData.setAllocatedSize(drbdVlmData.getAllocatedSize());
                updateUsableSizeFromAllocatedSize(dataChildVlmData);

                long grossSize = dataChildVlmData.getUsableSize();

                if (drbdVlmData.isUsingExternalMetaData())
                {
                    // calculate extMetaSize
                    long extMdSize;

                    VlmProviderObject<?> metaChild = drbdVlmData.getChildBySuffix(
                        RscLayerSuffixes.SUFFIX_DRBD_META
                    );
                    if (metaChild != null)
                    {
                        // is null if we are nvme-traget while the drbd-ext-metadata stays on the initiator side
                        extMdSize = new MetaData().getExternalMdSize(
                            grossSize,
                            peerSlots,
                            FIXME_AL_STRIPES,
                            FIXME_AL_STRIPE_SIZE
                        );

                        // even if we are updating fromAllocated, extMetaData still needs to be calculated fromUsable
                        metaChild.setUsableSize(extMdSize);
                        updateAllocatedSizeFromUsableSize(metaChild);
                    }
                    else
                    {
                        extMdSize = 0;
                    }
                    drbdVlmData.setUsableSize(grossSize);
                    drbdVlmData.setAllocatedSize(grossSize + extMdSize);
                }
                else
                {
                    long netSize = new MetaData().getNetSize(
                        grossSize,
                        peerSlots,
                        FIXME_AL_STRIPES,
                        FIXME_AL_STRIPE_SIZE
                    );
                    drbdVlmData.setUsableSize(netSize);
                    drbdVlmData.setAllocatedSize(grossSize);
                }
            }
        }
        catch (
            InvalidKeyException | IllegalArgumentException | MinSizeException | MaxSizeException | MinAlSizeException |
            MaxAlSizeException | AlStripesException | PeerCountException exc
        )
        {
            throw new ImplementationError(exc);
        }
    }

    private boolean isDiskless(AbsResource<?> absRscRef) throws AccessDeniedException
    {
        boolean ret;
        if (absRscRef instanceof Snapshot)
        {
            ret = false; // we do not create snapshots from diskless resources :)
        }
        else
        {
            StateFlags<Flags> rscFlags = ((Resource) absRscRef).getStateFlags();
            ret = rscFlags.isSet(sysCtx, Resource.Flags.DRBD_DISKLESS) &&
                !rscFlags.isSomeSet(sysCtx, Resource.Flags.DISK_ADD_REQUESTED, Resource.Flags.DISK_ADDING);
        }
        return ret;
    }

}
