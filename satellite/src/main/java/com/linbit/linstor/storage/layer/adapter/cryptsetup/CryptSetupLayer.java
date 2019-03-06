package com.linbit.linstor.storage.layer.adapter.cryptsetup;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupRscData;
import com.linbit.linstor.storage.data.adapter.cryptsetup.CryptSetupVlmData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
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
public class CryptSetupLayer implements DeviceLayer
{
    private static final String CRYPT_IDENTIFIER_FORMAT = "crypt_%s_%05d";

    // linstor calculates in KiB
    private static final int MIB = 1024;

    private final AccessContext sysCtx;
    private final CryptSetupCommands cryptSetup;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;

    @Inject
    public CryptSetupLayer(
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
                    // we could check here if one of your CryptVlms is open - but this method is only called
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
        CryptSetupVlmData cryptData = (CryptSetupVlmData) vlmData;

        long parentAllocatedSize = cryptData.getParentAllocatedSizeOrElse(
            () -> cryptData.getVolume().getVolumeDefinition().getVolumeSize(sysCtx)
        );
        cryptData.setUsableSize(parentAllocatedSize);
        cryptData.setAllocatedSize(parentAllocatedSize + 2 * MIB);
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
        CryptSetupRscData cryptRscData = (CryptSetupRscData) rscData;
        boolean deleteRsc = cryptRscData.getResource().getStateFlags().isSet(sysCtx, RscFlags.DELETE);

        Map<Boolean, List<CryptSetupVlmData>> groupedByDeleteFlag =
            cryptRscData.getVlmLayerObjects().values().stream().collect(
                Collectors.partitioningBy(cryptVlmData ->
                {
                    boolean ret;
                    try
                    {
                        ret = deleteRsc || cryptVlmData.getVolume().getFlags().isSet(sysCtx, VlmFlags.DELETE);
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    return ret;
                }
            )
        );

        for (CryptSetupVlmData vlmData : groupedByDeleteFlag.get(true))
        {
            String identifier = getIdentifier(vlmData);

            if (cryptSetup.isOpen(identifier))
            {
                cryptSetup.closeCryptDevice(identifier);
                vlmData.setOpened(false);
            }
        }

        resourceProcessorProvider.get().process(
            rscData.getSingleChild(),
            snapshots,
            apiCallRc
        );

        for (CryptSetupVlmData vlmData : groupedByDeleteFlag.get(false))
        {
            String identifier = getIdentifier(vlmData);

            boolean isOpen = cryptSetup.isOpen(identifier);

            vlmData.setBackingDevice(vlmData.getSingleChild().getDevicePath());

            boolean alreadyLuks = cryptSetup.hasLuksFormat(vlmData);

            if (!alreadyLuks)
            {
                String providedDev = cryptSetup.createCryptDevice(
                    vlmData.getBackingDevice(),
                    vlmData.getEncryptedPassword(),
                    identifier
                );
                vlmData.setDevicePath(providedDev);
            }
            else
            {
                /*
                 * TODO: this step should not be necessary
                 *
                 * currently it is, because LayeredResourceHelper re-creates the  CryptSetupStltData
                 * in every iteration. Once those data live longer (or are restored from props)
                 * the next command can be removed
                 */
                vlmData.setDevicePath(cryptSetup.getCryptVolumePath(identifier));
            }

            if (!isOpen)
            {
                cryptSetup.openCryptDevice(vlmData.getBackingDevice(), identifier, vlmData.getEncryptedPassword());
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
        }
    }

    private String getIdentifier(CryptSetupVlmData vlmData)
    {
        String identifier = vlmData.getIdentifier();

        if (identifier == null)
        {
            identifier = String.format(
                CRYPT_IDENTIFIER_FORMAT,
                vlmData.getRscLayerObject().getSuffixedResourceName(),
                vlmData.getVlmNr().value
            );
            vlmData.setIdentifier(identifier);
        }

        return identifier;
    }
}
