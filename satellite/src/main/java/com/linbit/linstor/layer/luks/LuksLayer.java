package com.linbit.linstor.layer.luks;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.DeviceHandler.CloneStrategy;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.dmsetup.DmSetupUtils;
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
import com.linbit.linstor.storage.utils.Commands;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

@Singleton
public class LuksLayer implements DeviceLayer
{
    private static final String LUKS_IDENTIFIER_FORMAT = "%s_%05d";

    // always use DD as it keeps the resource thin in the beginning
    private static final Set<DeviceHandler.CloneStrategy> SUPPORTED_CLONE_STRATS = Collections.singleton(
        DeviceHandler.CloneStrategy.DD
    );

    private final AccessContext sysCtx;
    private final CryptSetupCommands cryptSetup;
    private final Provider<DeviceHandler> resourceProcessorProvider;
    private final ExtCmdFactory extCmdFactory;
    private final ErrorReporter errorReporter;
    private final StltSecurityObjects secObjs;
    private final DecryptionHelper decryptionHelper;

    @Inject
    public LuksLayer(
        @DeviceManagerContext AccessContext sysCtxRef,
        CryptSetupCommands cryptSetupRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorRef,
        ErrorReporter errorReporterRef,
        StltExtToolsChecker extToolsCheckerRef,
        StltSecurityObjects secObjsRef,
        DecryptionHelper decryptionHelperRef
    )
    {
        sysCtx = sysCtxRef;
        cryptSetup = cryptSetupRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorRef;
        errorReporter = errorReporterRef;
        secObjs = secObjsRef;
        decryptionHelper = decryptionHelperRef;
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
                    // no (drbd) connections to peers
                    Collections.emptyMap(),
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
    public void clearCache() throws StorageException
    {
        // no-op
    }

    @Override
    public @Nullable LocalPropsChangePojo setLocalNodeProps(Props localNodeProps)
    {
        // no-op
        return null;
    }

    @Override
    public boolean isSuspendIoSupported()
    {
        return true;
    }

    @Override
    public void suspendIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        DmSetupUtils.suspendIo(
            errorReporter,
            extCmdFactory,
            rscDataRef,
            true,
            null
        );
    }

    @Override
    public void resumeIo(AbsRscLayerObject<Resource> rscDataRef)
        throws ExtCmdFailedException, StorageException
    {
        DmSetupUtils.suspendIo(errorReporter, extCmdFactory, rscDataRef, false, null);
    }

    @Override
    public void updateSuspendState(AbsRscLayerObject<Resource> rscDataRef)
        throws DatabaseException, ExtCmdFailedException
    {
        rscDataRef.setIsSuspended(DmSetupUtils.isSuspended(extCmdFactory, rscDataRef));
    }

    @Override
    public void processResource(
        AbsRscLayerObject<Resource> rscData,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException, ResourceException, VolumeException, AccessDeniedException, DatabaseException
    {
        LuksRscData<Resource> luksRscData = (LuksRscData<Resource>) rscData;
        StateFlags<Flags> flags = luksRscData.getAbsResource().getStateFlags();
        boolean deleteRsc = flags.isSet(sysCtx, Resource.Flags.DELETE);
        boolean deactivateRsc = flags.isSet(sysCtx, Resource.Flags.INACTIVE);

        boolean failedMissingDecryptedPassphrase = false;

        for (LuksVlmData<Resource> vlmData : luksRscData.getVlmLayerObjects().values())
        {
            boolean deleteVlm = deleteRsc ||
                ((Volume) vlmData.getVolume()).getFlags()
                    .isSomeSet(
                        sysCtx,
                        Volume.Flags.DELETE,
                        Volume.Flags.CLONING
                    );
            vlmData.setDataDevice(vlmData.getSingleChild().getDevicePath());

            // if modify password is set, we are supposed to change the luks password to it.
            if (vlmData.getModifyPassword() != null)
            {
                if (!Arrays.equals(vlmData.getModifyPassword(), vlmData.getEncryptedKey()))
                {
                    byte[] masterKey = secObjs.getCryptKey();
                    try
                    {
                        byte[] plainModifyPassword = decryptionHelper.decrypt(masterKey, vlmData.getModifyPassword());
                        cryptSetup.changeKey(
                            vlmData.getDataDevice(), vlmData.getDecryptedPassword(), plainModifyPassword);

                        vlmData.setEncryptedKey(vlmData.getModifyPassword());
                        vlmData.setDecryptedPassword(plainModifyPassword);
                    }
                    catch (LinStorException exc)
                    {
                        throw new StorageException("Unable to decrypt modify password key.", exc);
                    }
                }

                vlmData.setModifyPassword(null);
            }

            byte[] decryptedPassphrase = vlmData.getDecryptedPassword();
            if (deleteVlm || deactivateRsc)
            {
                /*
                 * checking if the luks device is open or to close it does *not* require the volume's (decrypted)
                 * passphrase to be set. We can freely run those operations.
                 */
                String identifier = getIdentifier(vlmData);

                if (cryptSetup.isOpen(identifier))
                {
                    cryptSetup.closeLuksDevice(identifier);
                    vlmData.setOpened(false);
                    vlmData.setExists(false);

                    if (deleteVlm)
                    {
                        cryptSetup.deleteHeaders(vlmData.getDataDevice());
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
                if (((Volume) vlmData.getVolume()).getFlags().isSet(sysCtx, Volume.Flags.RESIZE) &&
                    vlmData.getSizeState().equals(Size.TOO_LARGE))
                {
                    String identifier = getIdentifier(vlmData);

                    boolean isOpen = cryptSetup.isOpen(identifier);

                    boolean alreadyLuks = cryptSetup.hasLuksFormat(vlmData);

                    if (isOpen && alreadyLuks)
                    {
                        if (decryptedPassphrase != null)
                        {
                            cryptSetup.shrink(vlmData, vlmData.getDecryptedPassword());
                            // do not set Size.AS_EXPECTED as we will need to grow as much as we can
                            // once the layers below us are done shrinking
                            // vlmData.setSizeState(Size.AS_EXPECTED);
                        }
                        else
                        {
                            failedMissingDecryptedPassphrase = true;
                            vlmData.setFailed(true);
                        }
                    }
                }
            }
        }

        resourceProcessorProvider.get().processResource(rscData.getSingleChild(), apiCallRc);

        for (LuksVlmData<Resource> vlmData : luksRscData.getVlmLayerObjects().values())
        {
            boolean deleteVlm = deleteRsc ||
                ((Volume) vlmData.getVolume()).getFlags()
                    .isSomeSet(
                        sysCtx,
                        Volume.Flags.DELETE,
                        Volume.Flags.CLONING
                    );
            byte[] decryptedPassphrase = vlmData.getDecryptedPassword();

            // re-set the devicePath in case we just created the child-layer
            // or the child-layer did not provide a dataDevice until now...
            vlmData.setDataDevice(vlmData.getSingleChild().getDevicePath());

            if (!deleteVlm && !deactivateRsc)
            {
                String identifier = getIdentifier(vlmData);

                boolean isOpen = cryptSetup.isOpen(identifier);

                boolean alreadyLuks = cryptSetup.hasLuksFormat(vlmData);

                if (!alreadyLuks)
                {
                    if (decryptedPassphrase != null)
                    {
                        String providedDev = cryptSetup.createLuksDevice(
                            vlmData.getDataDevice(),
                            vlmData.getDecryptedPassword(),
                            identifier
                        );
                        vlmData.setDevicePath(providedDev);

                        // prevent unnecessary resize when we just created the device
                        vlmData.setSizeState(Size.AS_EXPECTED);
                    }
                    else
                    {
                        failedMissingDecryptedPassphrase = true;
                        vlmData.setFailed(true);
                    }
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
                    if (decryptedPassphrase != null)
                    {
                        cryptSetup.openLuksDevice(
                            vlmData.getDataDevice(),
                            identifier,
                            vlmData.getDecryptedPassword(),
                            false
                        );
                    }
                    else
                    {
                        failedMissingDecryptedPassphrase = true;
                        vlmData.setFailed(true);
                    }
                }

                if (((Volume) vlmData.getVolume()).getFlags().isSet(sysCtx, Volume.Flags.RESIZE) &&
                    !vlmData.getSizeState().equals(Size.AS_EXPECTED))
                {
                    if (decryptedPassphrase != null)
                    {
                        cryptSetup.grow(vlmData, vlmData.getDecryptedPassword());
                    }
                    else
                    {
                        failedMissingDecryptedPassphrase = true;
                        vlmData.setFailed(true);
                    }
                }

                vlmData.setAllocatedSize(
                    Commands.getBlockSizeInKib(
                        extCmdFactory.create(),
                        vlmData.getDataDevice()
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
        if (failedMissingDecryptedPassphrase)
        {
            throw new AbortLayerProcessingException(
                rscData,
                String.format(
                    "Luks layer cannot process resource '%s' because some volumes " +
                        "are missing the decrypted key. Is the master key set?",
                    rscData.getSuffixedResourceName()
                )
            );
        }
    }

    @Override
    public boolean isDeleteFlagSet(AbsRscLayerObject<?> rscDataRef)
    {
        return false; // no layer specific DELETE flag
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

    @Override
    public CloneSupportResult getCloneSupport(
        AbsRscLayerObject<?> sourceRscLayerObjectRef,
        AbsRscLayerObject<?> targetRscLayerObjectRef
    )
    {
        // unconditionally. Does not matter if LUKS -> LUKS, LUKS -> non-LUKS or non-LUKS -> LUKS. All cases need to be
        // read and/or written through the LUKS layer.
        return CloneSupportResult.TRUE;
    }

    @Override
    public Set<CloneStrategy> getCloneStrategy(VlmProviderObject<?> vlm) throws StorageException
    {
        return SUPPORTED_CLONE_STRATS;
    }

    @Override
    public void openDeviceForClone(VlmProviderObject<?> vlm, @Nullable String targetRscNameRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        LuksVlmData<Resource> luksVlmData = (LuksVlmData<Resource>) vlm;
        String vlmIdentifier = getIdentifier(luksVlmData);
        String identifier = targetRscNameRef ==  null ?
            vlmIdentifier : vlmIdentifier + InternalApiConsts.CLONE_FOR_PREFIX + targetRscNameRef;
        boolean isOpen = cryptSetup.isOpen(identifier);
        if (!isOpen)
        {
            VlmProviderObject<?> storageChild = ((LuksVlmData<?>) vlm).getSingleChild();
            resourceProcessorProvider.get().openForClone(storageChild, targetRscNameRef);

            if (targetRscNameRef == null)
            {
                // initialize luks device
                cryptSetup.createLuksDevice(
                    storageChild.getCloneDevicePath(),
                    luksVlmData.getDecryptedPassword(),
                    identifier
                );
            }

            cryptSetup.openLuksDevice(
                storageChild.getCloneDevicePath(),
                identifier,
                luksVlmData.getDecryptedPassword(),
                targetRscNameRef != null
            );
        }
        else
        {
            // target should not have existed and source should be a snapshot
            errorReporter.logWarning("LuksLayer:openDeviceForClone: already open %s",
                cryptSetup.getLuksVolumePath(identifier));
        }
        vlm.setCloneDevicePath(cryptSetup.getLuksVolumePath(identifier));
    }

    @Override
    public void closeDeviceForClone(VlmProviderObject<?> vlm, @Nullable String targetRscNameRef) throws StorageException
    {
        LuksVlmData<Resource> luksVlmData = (LuksVlmData<Resource>) vlm;
        String vlmIdentifier = getIdentifier(luksVlmData);
        String identifier = targetRscNameRef ==  null ?
            vlmIdentifier : vlmIdentifier + InternalApiConsts.CLONE_FOR_PREFIX + targetRscNameRef;
        boolean isOpen = cryptSetup.isOpen(identifier);
        if (isOpen)
        {
            cryptSetup.closeLuksDevice(identifier);
        }
        resourceProcessorProvider.get().closeAfterClone(((LuksVlmData<?>) vlm).getSingleChild(), targetRscNameRef);
        vlm.setCloneDevicePath(null);
    }
}
