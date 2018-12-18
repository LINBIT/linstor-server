package com.linbit.linstor.storage.layer.adapter.cryptsetup;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume;
import com.linbit.linstor.Volume.VlmFlags;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.Resource.RscFlags;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.core.devmgr.DeviceHandler2;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.ResourceLayer;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;
import com.linbit.linstor.storage.layer.provider.utils.Commands;
import com.linbit.linstor.storage.utils.CryptSetup;
import com.linbit.linstor.storage.utils.ResourceUtils;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
public class CryptSetupLayer implements ResourceLayer
{
    private static final String CRYPT_IDENTIFIER_FORMAT = "crypt_%s_%05d";

    // linstor calculates in KiB
    private static final int MIB = 1024;

    private final AccessContext sysCtx;
    private final CryptSetup cryptSetup;
    private final Provider<DeviceHandler2> resourceProcessorProvider;
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;

    @Inject
    public CryptSetupLayer(
        @DeviceManagerContext AccessContext sysCtxRef,
        CryptSetup cryptSetupRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler2> resourceProcessorRef,
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
    public void prepare(List<Resource> rscs, List<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (Volume vlm : rscs.stream().flatMap(Resource::streamVolumes).collect(Collectors.toList()))
        {
            CryptSetupStltData data = (CryptSetupStltData) vlm.getLayerData(sysCtx);
            if (data == null)
            {
                VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
                vlm.setLayerData(
                    sysCtx,
                    new CryptSetupStltData(
                        vlmDfn.getCryptKey(sysCtx).getBytes(),
                        String.format(
                            CRYPT_IDENTIFIER_FORMAT,
                            vlm.getResourceDefinition().getName().displayValue,
                            vlmDfn.getVolumeNumber().value
                        )
                    )
                );
System.out.println("created CryptSetupData for " + vlm.getKey());
            }
        }
        // ignore snapshots
    }

    @Override
    public void updateGrossSize(Volume cryptVlm, Volume parentVolume) throws AccessDeniedException, SQLException
    {
        cryptVlm.setUsableSize(sysCtx, parentVolume.getAllocatedSize(sysCtx));
        cryptVlm.setAllocatedSize(sysCtx, parentVolume.getAllocatedSize(sysCtx) + 2 * MIB);
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
    public void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, SQLException
    {
        Resource childRsc = ResourceUtils.getSingleChild(rsc, sysCtx);

        boolean deleteRsc = rsc.getStateFlags().isSet(sysCtx, RscFlags.DELETE);

        Map<Boolean, List<Volume>> groupedByDeleteFlag =
            rsc.streamVolumes().collect(
                Collectors.partitioningBy(vlm ->
                {
                    boolean ret;
                    try
                    {
                        ret = deleteRsc || vlm.getFlags().isSet(sysCtx, VlmFlags.DELETE);
                    }
                    catch (AccessDeniedException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                    return ret;
                }
            )
        );

        for (Volume vlm : groupedByDeleteFlag.get(true))
        {
            CryptSetupStltData data = (CryptSetupStltData) vlm.getLayerData(sysCtx);
            String identifier = getIdentifier(vlm, data);

System.out.println("checkig if '" + identifier + "' is open... ");

            if (cryptSetup.isOpen(identifier))
            {
System.out.println("  open");
                cryptSetup.closeCryptDevice(identifier);
                data.opened = false;
            }
        }

        resourceProcessorProvider.get().process(
            childRsc,
            snapshots,
            apiCallRc
        );

        for (Volume vlm : groupedByDeleteFlag.get(false))
        {
            CryptSetupStltData data = (CryptSetupStltData) vlm.getLayerData(sysCtx);

            String identifier = getIdentifier(vlm, data);

            boolean isOpen = cryptSetup.isOpen(identifier);

            String backingDev = childRsc.getVolume(
                vlm.getVolumeDefinition().getVolumeNumber()
            ).getDevicePath(sysCtx);

            vlm.setBackingDiskPath(sysCtx, backingDev);

            boolean alreadyLuks = cryptSetup.hasLuksFormat(backingDev);

            if (!alreadyLuks)
            {
                String providedDev = cryptSetup.createCryptDevice(backingDev, data.password, identifier);
                vlm.setDevicePath(sysCtx, providedDev);
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
                vlm.setDevicePath(sysCtx, cryptSetup.getCryptVolumePath(identifier));
            }

            if (!isOpen)
            {
                cryptSetup.openCryptDevice(backingDev, identifier, data.password);
            }

            vlm.setAllocatedSize(
                sysCtx,
                Commands.getBlockSizeInKib(
                    extCmdFactory.create(),
                    backingDev
                )
            );
            vlm.setUsableSize(
                sysCtx,
                Commands.getBlockSizeInKib(
                    extCmdFactory.create(),
                    vlm.getDevicePath(sysCtx)
                )
            );

            data.opened = true;
        }
    }

    private String getIdentifier(Volume vlm, CryptSetupStltData vlmData)
    {
        String identifier = vlmData.identifier;

        if (identifier == null)
        {
            identifier = String.format(
                CRYPT_IDENTIFIER_FORMAT,
                vlm.getResourceDefinition().getName().displayValue,
                vlm.getVolumeDefinition().getVolumeNumber().value
            );
            vlmData.identifier = identifier;
        }

        return identifier;
    }

    private boolean isDeviceAlreadyLuks(String providedDev, String backingDev)
    {
        /*
         * check with "cryptsetup isLuks <device>" if the backing device is already in luks format
         * retcode 0 == yes, 1 == no
         */
        throw new ImplementationError("Not implemented yet");
    }

    private void cryptLuksFormat(String backingDev)
    {
        /*
         * if not in luksformat exec "cryptsetup -q luksFormat <device>" and write the passwd
         * to the cryptsetup's stdin
         */
        throw new ImplementationError("Not implemented yet");
    }


    private void closeLuks(String luksIdentifier)
    {
        /*
         *
         */
        throw new ImplementationError("Not implemented yet");
    }

}
