package com.linbit.linstor.layer.luks;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.storage.utils.Commands;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.stateflags.StateFlags;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.List;
import java.util.Set;

@Singleton
public class LuksLayer implements DeviceLayer
{
    private static final String LUKS_IDENTIFIER_FORMAT = "%s_%05d";

    // linstor calculates in KiB
    private static final int MIB = 1024;

    private final AccessContext sysCtx;
    private final CryptSetupCommands cryptSetup;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;

    @Inject
    public LuksLayer(
        @DeviceManagerContext AccessContext sysCtxRef,
        CryptSetupCommands cryptSetupRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorRef,
        ErrorReporter errorReporterRef
    )
    {
        sysCtx = sysCtxRef;
        cryptSetup = cryptSetupRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorRef;
        errorReporter = errorReporterRef;
    }

    @Override
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    @Override
    public void prepare(
        Set<AbsRscLayerObject<Resource>> rscDataList,
        Set<AbsRscLayerObject<Snapshot>> affectedSnapshots
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // no-op
    }

    @Override
    public boolean resourceFinished(AbsRscLayerObject<Resource> layerDataRef) throws AccessDeniedException
    {
        StateFlags<Flags> rscFlags = layerDataRef.getAbsResource().getStateFlags();
        if (rscFlags.isSet(sysCtx, Resource.Flags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(layerDataRef);
        }
        else
        {
            boolean isActive = rscFlags.isSet(sysCtx, Resource.Flags.INACTIVE);
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                layerDataRef,
                new ResourceState(
                    isActive,
                    // we could check here if one of your LuksVlms is open - but this method is only called
                    // right after the creation, where nothing can be in use now.
                    false,
                    isActive,
                    null,
                    null
                )
            );
        }
        return true;
    }

    @Override
    public void updateAllocatedSizeFromUsableSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        LuksVlmData<Resource> luksData = (LuksVlmData<Resource>) vlmData;
        long grossSize = luksData.getUsableSize() + 16 * MIB;
        luksData.setAllocatedSize(grossSize);

        VlmProviderObject<Resource> childVlmData = luksData.getSingleChild();
        childVlmData.setUsableSize(grossSize);
        resourceProcessorProvider.get().updateAllocatedSizeFromUsableSize(childVlmData);
    }

    @Override
    public void updateUsableSizeFromAllocatedSize(VlmProviderObject<Resource> vlmData)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        LuksVlmData<Resource> luksData = (LuksVlmData<Resource>) vlmData;
        long grossSize = luksData.getAllocatedSize();
        long netSize = grossSize - 16 * MIB;

        luksData.setUsableSize(netSize);

        VlmProviderObject<Resource> childVlmData = luksData.getSingleChild();
        childVlmData.setUsableSize(grossSize);
        resourceProcessorProvider.get().updateUsableSizeFromAllocatedSize(childVlmData);
    }

    @Override
    public void clearCache() throws StorageException
    {
        // no-op
    }

    @Override
    public void setLocalNodeProps(Props localNodeProps)
    {
        // no-op
    }

    @Override
    public LayerProcessResult process(
        AbsRscLayerObject<Resource> rscData,
        List<Snapshot> snapshotList,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        LayerProcessResult ret;
        LuksRscData<Resource> luksRscData = (LuksRscData<Resource>) rscData;
        StateFlags<Flags> flags = luksRscData.getAbsResource().getStateFlags();
        boolean deleteRsc = flags.isSet(sysCtx, Resource.Flags.DELETE);
        boolean deactivateRsc = flags.isSet(sysCtx, Resource.Flags.INACTIVE);

        boolean allVolumeKeysDecrypted = true;
        for (LuksVlmData<Resource> vlmData : luksRscData.getVlmLayerObjects().values())
        {
            if (vlmData.getDecryptedPassword() == null)
            {
                allVolumeKeysDecrypted = false;
                break;
            }
        }

        if (allVolumeKeysDecrypted) // otherwise do not even process children.
        {
            for (LuksVlmData<Resource> vlmData : luksRscData.getVlmLayerObjects().values())
            {
                boolean deleteVlm = deleteRsc ||
                    ((Volume) vlmData.getVolume()).getFlags().isSet(sysCtx, Volume.Flags.DELETE);
                if (deleteVlm || deactivateRsc)
                {
                    String identifier = getIdentifier(vlmData);

                    if (cryptSetup.isOpen(identifier))
                    {
                        cryptSetup.closeLuksDevice(identifier);
                        vlmData.setOpened(false);
                        vlmData.setExists(false);

                        if (deleteVlm)
                        {
                            cryptSetup.deleteHeaders(vlmData.getBackingDevice());
                        }
                        else
                        {
                            vlmData.setDevicePath(null);
                        }
                    }
                }
                else
                {
                    // shrink if necessary
                    if (
                        ((Volume) vlmData.getVolume()).getFlags().isSet(sysCtx, Volume.Flags.RESIZE) &&
                            vlmData.getSizeState().equals(Size.TOO_LARGE)
                    )
                    {
                        String identifier = getIdentifier(vlmData);

                        boolean isOpen = cryptSetup.isOpen(identifier);

                        vlmData.setBackingDevice(vlmData.getSingleChild().getDevicePath());

                        boolean alreadyLuks = cryptSetup.hasLuksFormat(vlmData);

                        if (isOpen && alreadyLuks)
                        {
                            cryptSetup.shrink(vlmData);
                            // do not set Size.AS_EXPECTED as we will need to grow as much as we can
                            // once the layers below us are done shrinking
                            // vlmData.setSizeState(Size.AS_EXPECTED);
                        }
                    }
                }
            }

            LayerProcessResult processResult = resourceProcessorProvider.get().process(
                rscData.getSingleChild(),
                snapshotList,
                apiCallRc
            );

            if (processResult == LayerProcessResult.SUCCESS)
            {
                for (LuksVlmData<Resource> vlmData : luksRscData.getVlmLayerObjects().values())
                {
                    boolean deleteVlm = deleteRsc ||
                        ((Volume) vlmData.getVolume()).getFlags().isSet(sysCtx, Volume.Flags.DELETE);
                    if (!deleteVlm && !deactivateRsc)
                    {
                        String identifier = getIdentifier(vlmData);

                        boolean isOpen = cryptSetup.isOpen(identifier);

                        vlmData.setBackingDevice(vlmData.getSingleChild().getDevicePath());

                        boolean alreadyLuks = cryptSetup.hasLuksFormat(vlmData);

                        if (!alreadyLuks)
                        {
                            String providedDev = cryptSetup.createLuksDevice(
                                vlmData.getBackingDevice(),
                                vlmData.getDecryptedPassword(),
                                identifier
                            );
                            vlmData.setDevicePath(providedDev);

                            // prevent unnecessary resize when we just created the device
                            vlmData.setSizeState(Size.AS_EXPECTED);
                        }
                        else
                        {
                            /*
                             * TODO: this step should not be necessary
                             * currently it is, because LayeredResourceHelper re-creates the LuksVlmData
                             * in every iteration. Once those data live longer (or are restored from props)
                             * the next command can be removed
                             */
                            vlmData.setDevicePath(cryptSetup.getLuksVolumePath(identifier));
                        }

                        if (!isOpen)
                        {
                            cryptSetup
                                .openLuksDevice(vlmData.getBackingDevice(), identifier, vlmData.getDecryptedPassword());
                        }

                        if (
                            ((Volume) vlmData.getVolume()).getFlags().isSet(sysCtx, Volume.Flags.RESIZE) &&
                                !vlmData.getSizeState().equals(Size.AS_EXPECTED)
                        )
                        {
                            cryptSetup.grow(vlmData);
                        }

                        vlmData.setAllocatedSize(
                            Commands.getBlockSizeInKib(
                                extCmdFactory.create(),
                                vlmData.getBackingDevice()
                            )
                        );
                        vlmData.setUsableSize(
                            Commands.getBlockSizeInKib(
                                extCmdFactory.create(),
                                vlmData.getDevicePath()
                            )
                        );
                        vlmData.setSizeState(Size.AS_EXPECTED);

                        vlmData.setOpened(true);
                        vlmData.setFailed(false);
                    }
                }
                ret = LayerProcessResult.SUCCESS;
            }
            else
            {
                ret = processResult;
            }
        }
        else
        {
            for (LuksVlmData<Resource> vlmData : luksRscData.getVlmLayerObjects().values())
            {
                vlmData.setFailed(true);
            }
            throw new AbortLayerProcessingException(
                luksRscData,
                String.format(
                    "Luks layer cannot process resource '%s' because some volumes " +
                        "are missing the decrypted key. Is the master key set?",
                    luksRscData.getSuffixedResourceName()
                )
            );
        }
        return ret;
    }

    private String getIdentifier(LuksVlmData<Resource> vlmData)
    {
        String identifier = vlmData.getIdentifier();

        if (identifier == null)
        {
            identifier = String.format(
                LUKS_IDENTIFIER_FORMAT,
                vlmData.getRscLayerObject().getSuffixedResourceName(),
                vlmData.getVlmNr().value
            );
            vlmData.setIdentifier(identifier);
        }

        return identifier;
    }
}
