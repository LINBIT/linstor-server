package com.linbit.linstor.storage.layer.adapter.luks;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.event.common.UsageState;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.utils.Commands;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    public void prepare(Set<RscLayerObject> rscDataList, Set<Snapshot> affectedSnapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    public void resourceFinished(RscLayerObject layerDataRef) throws AccessDeniedException
    {
        if (layerDataRef.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE))
        {
            resourceProcessorProvider.get().sendResourceDeletedEvent(layerDataRef);
        }
        else
        {
            resourceProcessorProvider.get().sendResourceCreatedEvent(
                layerDataRef,
                new UsageState(
                    true,
                    // we could check here if one of your LuksVlms is open - but this method is only called
                    // right after the creation, where nothing can be in use now.
                    false,
                    true
                )
            );
        }
    }

    @Override
    public void updateGrossSize(VlmProviderObject vlmData) throws AccessDeniedException, SQLException
    {
        LuksVlmData luksData = (LuksVlmData) vlmData;

        long size = luksData.getUsableSize() + 2 * MIB;
        luksData.setAllocatedSize(size);

        luksData.getSingleChild().setUsableSize(size);
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
    public void process(RscLayerObject rscData, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        LuksRscData luksRscData = (LuksRscData) rscData;
        boolean deleteRsc = luksRscData.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE);

        Map<Boolean, List<LuksVlmData>> groupedByDeleteFlag =
            luksRscData.getVlmLayerObjects().values().stream().collect(
                Collectors.partitioningBy(luksVlmData ->
                {
                    boolean ret;
                    try
                    {
                        ret = deleteRsc || luksVlmData.getVolume().getFlags().isSet(sysCtx, VlmFlags.DELETE);
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    return ret;
                }
            )
        );

        boolean allVolumeKeysDecrypted = true;
        for (LuksVlmData vlmData : luksRscData.getVlmLayerObjects().values())
        {
            if (vlmData.getDecryptedPassword() == null)
            {
                allVolumeKeysDecrypted = false;
                break;
            }
        }

        if (allVolumeKeysDecrypted) // otherwise do not even process children.
        {
            for (LuksVlmData vlmData : groupedByDeleteFlag.get(true))
            {
                String identifier = getIdentifier(vlmData);

                if (cryptSetup.isOpen(identifier))
                {
                    cryptSetup.closeLuksDevice(identifier);
                    vlmData.setOpened(false);
                    vlmData.setExists(false);
                    cryptSetup.deleteHeaders(vlmData.getBackingDevice());
                }
            }

            resourceProcessorProvider.get().process(
                rscData.getSingleChild(),
                snapshots,
                apiCallRc
            );

            for (LuksVlmData vlmData : groupedByDeleteFlag.get(false))
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
                }
                else
                {
                    /*
                     * TODO: this step should not be necessary
                     *
                     * currently it is, because LayeredResourceHelper re-creates the LuksVlmData
                     * in every iteration. Once those data live longer (or are restored from props)
                     * the next command can be removed
                     */
                    vlmData.setDevicePath(cryptSetup.getLuksVolumePath(identifier));
                }

                if (!isOpen)
                {
                    cryptSetup.openLuksDevice(vlmData.getBackingDevice(), identifier, vlmData.getDecryptedPassword());
                }

                vlmData.setAllocatedSize(Commands.getBlockSizeInKib(
                    extCmdFactory.create(),
                    vlmData.getBackingDevice()
                ));
                vlmData.setUsableSize(Commands.getBlockSizeInKib(
                    extCmdFactory.create(),
                    vlmData.getDevicePath()
                ));

                vlmData.setOpened(true);
                vlmData.setFailed(false);
            }
        }
        else
        {
            errorReporter.logWarning(
                "Luks layer cannot process resource '%s' because some volumes " +
                    "are missing the decrypted key. Is the master key set?",
                luksRscData.getSuffixedResourceName()
            );
            for (LuksVlmData vlmData : luksRscData.getVlmLayerObjects().values())
            {
                vlmData.setFailed(true);
            }
        }
    }

    private String getIdentifier(LuksVlmData vlmData)
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
