package com.linbit.linstor.layer.drbd;

import com.linbit.ImplementationError;
import com.linbit.drbd.md.AlStripesException;
import com.linbit.drbd.md.BitmapBlockSizeException;
import com.linbit.drbd.md.MaxAlSizeException;
import com.linbit.drbd.md.MaxSizeException;
import com.linbit.drbd.md.MdException;
import com.linbit.drbd.md.MetaData;
import com.linbit.drbd.md.MinAlSizeException;
import com.linbit.drbd.md.MinSizeException;
import com.linbit.drbd.md.PeerCountException;
import com.linbit.exceptions.InvalidSizeException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.SnapshotName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.AbsLayerSizeCalculator;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
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
public class DrbdLayerSizeCalculator extends AbsLayerSizeCalculator<DrbdVlmData<?>>
{
    // Number of activity log stripes for DRBD meta data; this should be replaced with a property of the
    // resource definition, a property of the volume definition, or otherwise a system-wide default
    public static final int FIXME_AL_STRIPES = 1;

    // Number of activity log stripes; this should be replaced with a property of the resource definition,
    // a property of the volume definition, or or otherwise a system-wide default
    public static final long FIXME_AL_STRIPE_SIZE = 32;

    private ErrorReporter errLog;

    @Inject
    public DrbdLayerSizeCalculator(AbsLayerSizeCalculatorInit initRef, ErrorReporter errLogRef)
    {
        super(initRef, DeviceLayerKind.DRBD);
        errLog = errLogRef;
    }

    @Override
    protected void updateAllocatedSizeFromUsableSizeImpl(DrbdVlmData<?> drbdVlmDataRef)
        throws AccessDeniedException, DatabaseException, InvalidSizeException
    {
        DrbdRscData<?> drbdRscData = drbdVlmDataRef.getRscLayerObject();
        short peerSlots = drbdRscData.getPeerSlots();
        final int bitmapBlockSizeKiB = getAndSetBitmapBlockSizeKiB(drbdVlmDataRef);

        try
        {
            boolean isDiskless = isDiskless(drbdVlmDataRef.getRscLayerObject().getAbsResource());
            if (!isDiskless && !drbdRscData.isSkipDiskEnabled(sysCtx, stltProps))
            {
                long netSize = drbdVlmDataRef.getUsableSize();

                VlmProviderObject<?> dataChild = drbdVlmDataRef.getChildBySuffix(RscLayerSuffixes.SUFFIX_DATA);
                if (drbdVlmDataRef.isUsingExternalMetaData())
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
                        FIXME_AL_STRIPE_SIZE,
                        bitmapBlockSizeKiB
                    );

                    VlmProviderObject<?> metaChild = drbdVlmDataRef
                        .getChildBySuffix(RscLayerSuffixes.SUFFIX_DRBD_META);
                    if (metaChild != null)
                    {
                        // is null if we are nvme-traget while the drbd-ext-metadata stays on the initiator side
                        metaChild.setUsableSize(extMdSize);
                        updateAllocatedSizeFromUsableSize(metaChild);
                    }

                    drbdVlmDataRef.setAllocatedSize(netSize + extMdSize); // rough estimation
                }
                else
                {
                    long grossSize = new MetaData().getGrossSize(
                        netSize,
                        peerSlots,
                        FIXME_AL_STRIPES,
                        FIXME_AL_STRIPE_SIZE,
                        bitmapBlockSizeKiB
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
                        FIXME_AL_STRIPE_SIZE,
                        bitmapBlockSizeKiB
                    );

                    drbdVlmDataRef.setAllocatedSize(grossSize);
                }

                // we need to update the usable size once again since the layers below us
                // might have given us more data than we asked for.
                drbdVlmDataRef.setUsableSize(netSize);
            }
        }
        catch (InvalidKeyException | IllegalArgumentException | AlStripesException | PeerCountException exc)
        {
            throw new ImplementationError(exc);
        }
        catch (MinSizeException | MaxSizeException | MinAlSizeException | MaxAlSizeException exc)
        {
            throw new InvalidSizeException(drbdVlmDataRef.getVolumeKey() + "'s size is invalid", exc);
        }
        catch (BitmapBlockSizeException exc)
        {
            throw new InvalidSizeException(
                "The DRBD bitmap block size set on " + drbdVlmDataRef.getVolumeKey() +
                " is invalid",
                exc
            );
        }
    }

    @Override
    protected void updateUsableSizeFromAllocatedSizeImpl(DrbdVlmData<?> drbdVlmDataRef)
        throws AccessDeniedException, DatabaseException, InvalidSizeException
    {
        DrbdRscData<?> drbdRscData = drbdVlmDataRef.getRscLayerObject();
        short peerSlots = drbdRscData.getPeerSlots();
        final int bitmapBlockSizeKiB = getAndSetBitmapBlockSizeKiB(drbdVlmDataRef);

        try
        {
            boolean isDiskless = isDiskless(drbdVlmDataRef.getRscLayerObject().getAbsResource());
            if (!isDiskless && !drbdRscData.isSkipDiskEnabled(sysCtx, stltProps))
            {
                // let next layer calculate
                VlmProviderObject<?> dataChildVlmData = drbdVlmDataRef.getChildBySuffix(
                    RscLayerSuffixes.SUFFIX_DATA
                );
                dataChildVlmData.setAllocatedSize(drbdVlmDataRef.getAllocatedSize());
                updateUsableSizeFromAllocatedSize(dataChildVlmData);

                long grossSize = dataChildVlmData.getUsableSize();

                if (drbdVlmDataRef.isUsingExternalMetaData())
                {
                    // calculate extMetaSize
                    long extMdSize;

                    VlmProviderObject<?> metaChild = drbdVlmDataRef.getChildBySuffix(
                        RscLayerSuffixes.SUFFIX_DRBD_META
                    );
                    if (metaChild != null)
                    {
                        // is null if we are nvme-traget while the drbd-ext-metadata stays on the initiator side
                        extMdSize = new MetaData().getExternalMdSize(
                            grossSize,
                            peerSlots,
                            FIXME_AL_STRIPES,
                            FIXME_AL_STRIPE_SIZE,
                            bitmapBlockSizeKiB
                        );

                        // even if we are updating fromAllocated, extMetaData still needs to be calculated fromUsable
                        metaChild.setUsableSize(extMdSize);
                        updateAllocatedSizeFromUsableSize(metaChild);
                    }
                    else
                    {
                        extMdSize = 0;
                    }
                    drbdVlmDataRef.setUsableSize(grossSize);
                    drbdVlmDataRef.setAllocatedSize(grossSize + extMdSize);
                }
                else
                {
                    long netSize = new MetaData().getNetSize(
                        grossSize,
                        peerSlots,
                        FIXME_AL_STRIPES,
                        FIXME_AL_STRIPE_SIZE,
                        bitmapBlockSizeKiB
                    );
                    drbdVlmDataRef.setUsableSize(netSize);
                    drbdVlmDataRef.setAllocatedSize(grossSize);
                }
            }
        }
        catch (InvalidKeyException | IllegalArgumentException | MdException exc)
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

    /**
     * Gets the effective bitmap block size for this volume in KiB, and if it is not set on the Volume,
     * sets it on the Volume.
     */
    private int getAndSetBitmapBlockSizeKiB(final DrbdVlmData<?> drbdVlmDataRef)
        throws AccessDeniedException, DatabaseException
    {
        DrbdRscData rscData = drbdVlmDataRef.getRscLayerObject();

        @Nullable Props vlmProps = null;
        @Nullable Props rscProps = null;

        @Nullable Volume vlm = null;
        @Nullable Resource rsc = null;

        final AbsVolume absVlm = drbdVlmDataRef.getVolume();
        if (absVlm instanceof Volume tmpVlm)
        {
            vlm = tmpVlm;
            vlmProps = vlm.getProps(sysCtx);
        }

        final AbsResource absRsc = rscData.getAbsResource();
        if (absRsc instanceof Resource tmpRsc)
        {
            rsc = tmpRsc;
            rscProps = rsc.getProps(sysCtx);
        }

        int bitmapBlockSizeKiB = MetaData.DRBD_DEFAULT_BM_BIT_COVER_kiB;
        boolean haveBlockSize = false;
        boolean haveVlmProp = false;

        // Get the bitmap block size that was stored for this volume
        // Alternatively, get it from the resource that the volume is a part of. This is not normally used,
        // but may be useful for manual intervention, testing or debugging.
        {
            @Nullable String valueStr = null;
            if (vlmProps != null)
            {
                valueStr = vlmProps.getProp(ApiConsts.KEY_BITMAP_BLOCK_SIZE);
            }

            haveVlmProp = valueStr != null;
            if (!haveVlmProp)
            {
                if (rscProps != null)
                {
                    valueStr = rscProps.getProp(ApiConsts.KEY_BITMAP_BLOCK_SIZE);
                }
            }

            if (valueStr != null)
            {
                try
                {
                    bitmapBlockSizeKiB = Integer.parseInt(valueStr);
                    haveBlockSize = true;
                }
                catch (NumberFormatException ignored)
                {
                    @Nullable String objectId = objectIdFrom(absRsc, rsc);

                    final VolumeNumber vlmNr = vlm.getVolumeNumber();
                    errLog.logError(
                        "%s Volume %d: Property %s: Bitmap block size value is unparsable",
                        (objectId != null ? objectId : "<Unidentified object>"),
                        vlmNr.value,
                        ApiConsts.KEY_BITMAP_BLOCK_SIZE
                    );
                }
            }
        }

        // If there is no bitmap block size set on the volume or resource, inherit the setting
        // for creating new resources and their volumes from the resource group, resource definition
        // or volume definition (the former being overridden by the latter).
        if (!haveBlockSize)
        {
            @Nullable Props vlmDfnProps = null;
            @Nullable Props rscDfnProps = null;
            @Nullable Props rscGrpProps = null;

            if (vlm != null)
            {
                final VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                vlmDfnProps = vlmDfn.getProps(sysCtx);
            }

            @Nullable ResourceDefinition rscDfn = null;
            @Nullable ResourceGroup rscGrp = null;
            if (rsc != null)
            {
                rscDfn = rsc.getResourceDefinition();
                rscDfnProps = rscDfn.getProps(sysCtx);
                rscGrp = rscDfn.getResourceGroup();
                rscGrpProps = rscGrp.getProps(sysCtx);
            }

            PriorityProps prioProps = new PriorityProps(vlmDfnProps, rscDfnProps, rscGrpProps);
            final String valueStr = prioProps.getProp(ApiConsts.KEY_BITMAP_BLOCK_SIZE_NEW_RESOURCE);
            try
            {
                bitmapBlockSizeKiB = Integer.parseInt(valueStr);
                haveBlockSize = true;
            }
            catch (NumberFormatException ignored)
            {
                @Nullable String objectId = objectIdFrom(absRsc, rsc);

                if (objectId != null && vlm != null)
                {
                    final VolumeNumber vlmNr = vlm.getVolumeNumber();
                    objectId += " Volume " + vlmNr.value;
                }

                errLog.logError(
                    "Property %s inheritance order " +
                    "VolumeDefinition > ResourceDefinition > ResourceGroup for %s: " +
                    "Bitmap block size value is unparsable",
                    ApiConsts.KEY_BITMAP_BLOCK_SIZE_NEW_RESOURCE,
                    (objectId != null ? objectId : "<Unidentified object>")
                );
            }
        }

        try
        {
            MetaData.checkBitmapBlockSize(bitmapBlockSizeKiB);
        }
        catch (MdException exc)
        {
            final int invalidValue = bitmapBlockSizeKiB;

            // Fallback to the default value if the stored value is invalid
            bitmapBlockSizeKiB = MetaData.DRBD_DEFAULT_BM_BIT_COVER_kiB;

            @Nullable String objectId = null;
            if (rsc != null)
            {
                final Node rscNode = rsc.getNode();
                final NodeName rscNodeName = rscNode.getName();
                final ResourceDefinition rscDfn = rsc.getResourceDefinition();
                final ResourceName rscName = rscDfn.getName();
                objectId = String.format(
                    "Node %s Resource %s", rscNodeName.displayValue, rscName.displayValue
                );
            }
            else
            if (absRsc instanceof Snapshot tmpSnp)
            {
                final NodeName rscNodeName = tmpSnp.getNodeName();
                final ResourceName rscName = tmpSnp.getResourceName();
                final SnapshotName snpName = tmpSnp.getSnapshotName();
                objectId = String.format(
                    "Node %s Resource %s Snapshot %s",
                    rscNodeName.displayValue, rscName.displayValue, snpName.displayValue
                );
            }

            final VolumeNumber vlmNr = vlm.getVolumeNumber();
            errLog.logError(
                "%s Volume %d: Bitmap block size value of %d is invalid, fall back to default value %d",
                (objectId != null ? objectId : "<Unidentified object>"),
                vlmNr.value,
                invalidValue, bitmapBlockSizeKiB
            );
        }

        // If no bitmap block size was set on the volume, set whatever value was determined on the volume now
        if (!haveVlmProp && haveBlockSize && vlm != null)
        {
            setBitmapBlockSizeOnVolume(vlm, bitmapBlockSizeKiB);
        }
        return bitmapBlockSizeKiB;
    }

    private void setBitmapBlockSizeOnVolume(final Volume vlm, final int bitmapBlockSizeKiB)
        throws AccessDeniedException, DatabaseException
    {
        final Props vlmProps = vlm.getProps(sysCtx);
        final String blockSizeStr = Integer.toString(bitmapBlockSizeKiB);
        try
        {
            vlmProps.setProp(ApiConsts.KEY_BITMAP_BLOCK_SIZE, blockSizeStr);
        }
        catch (InvalidValueException invalidExc)
        {
            throw new ImplementationError(
                "The string for a valid calculated DRBD bitmap block size is an " +
                "invalid value for the property that is supposed to store it",
                invalidExc
            );
        }
    }

    private String objectIdFrom(final AbsResource<?> absRsc, final Resource rsc)
    {
        @Nullable String objectId = null;
        if (rsc != null)
        {
            final Node rscNode = rsc.getNode();
            final NodeName rscNodeName = rscNode.getName();
            final ResourceDefinition rscDfn = rsc.getResourceDefinition();
            final ResourceName rscName = rscDfn.getName();
            objectId = String.format(
                "Node %s Resource %s", rscNodeName.displayValue, rscName.displayValue
            );
        }
        else
        if (absRsc instanceof Snapshot tmpSnp)
        {
            final NodeName rscNodeName = tmpSnp.getNodeName();
            final ResourceName rscName = tmpSnp.getResourceName();
            final SnapshotName snpName = tmpSnp.getSnapshotName();
            objectId = String.format(
                "Node %s Resource %s Snapshot %s",
                rscNodeName.displayValue, rscName.displayValue, snpName.displayValue
            );
        }
        return objectId;
    }
}
