package com.linbit.linstor.layer.luks;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.extproc.ExtCmdFailedException;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.LinStorException;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.DecryptionHelper;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.StltSecurityObjects;
import com.linbit.linstor.core.devmgr.DeviceHandler;
import com.linbit.linstor.core.devmgr.DeviceHandler.CloneStrategy;
import com.linbit.linstor.core.devmgr.exceptions.ResourceException;
import com.linbit.linstor.core.devmgr.exceptions.VolumeException;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Resource.Flags;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.event.common.ResourceState;
import com.linbit.linstor.layer.DeviceLayer;
import com.linbit.linstor.layer.dmsetup.DmSetupUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
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
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.Commands;
import com.linbit.linstor.storage.utils.LayerUtils;
import com.linbit.linstor.utils.layer.LayerVlmUtils;
import com.linbit.utils.Base64;
import com.linbit.utils.ShellUtils;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Singleton
public class LuksLayer implements DeviceLayer
{
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
    private final StltConfigAccessor stltConfigAccessor;

    @Inject
    public LuksLayer(
        @DeviceManagerContext AccessContext sysCtxRef,
        CryptSetupCommands cryptSetupRef,
        ExtCmdFactory extCmdFactoryRef,
        Provider<DeviceHandler> resourceProcessorRef,
        ErrorReporter errorReporterRef,
        StltSecurityObjects secObjsRef,
        DecryptionHelper decryptionHelperRef,
        StltConfigAccessor stltConfigAccessorRef
    )
    {
        sysCtx = sysCtxRef;
        cryptSetup = cryptSetupRef;
        extCmdFactory = extCmdFactoryRef;
        resourceProcessorProvider = resourceProcessorRef;
        errorReporter = errorReporterRef;
        secObjs = secObjsRef;
        decryptionHelper = decryptionHelperRef;
        stltConfigAccessor = stltConfigAccessorRef;
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
    public void suspendIo(AbsRscLayerObject<Resource> rscDataRef, boolean ignoredAsRootLayerRef)
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
    public void resumeIo(AbsRscLayerObject<Resource> rscDataRef, boolean ignoredAsRootLayerRef)
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
        boolean deleteRsc = flags.isSomeSet(sysCtx, Resource.Flags.DELETE, Resource.Flags.DISK_REMOVING);
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

            @Nullable byte[] decryptedPassphrase = vlmData.getDecryptedPassword();
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

                    if (deleteVlm && !allStorageChildrenZfs(luksRscData))
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

        List<Volume> vlmsWithCorruptKey = new ArrayList<>();
        for (LuksVlmData<Resource> vlmData : luksRscData.getVlmLayerObjects().values())
        {
            Volume vlm = (Volume) vlmData.getVolume();
            boolean deleteVlm = deleteRsc ||
                vlm.getFlags()
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
                        List<String> additionalOptions = getCreateOptions(vlmData);

                        String providedDev = cryptSetup.createLuksDevice(
                            vlmData.getDataDevice(),
                            decryptedPassphrase,
                            identifier,
                            additionalOptions
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
                        List<String> additionalOptions = getOpenOptions(vlmData);

                        cryptSetup.openLuksDevice(
                            vlmData.getDataDevice(),
                            identifier,
                            decryptedPassphrase,
                            false,
                            additionalOptions
                        );
                        // Invalidate cached discGran so DeviceHandlerImpl re-runs lsblk and the
                        // controller's auto-rs-discard-granularity sees the new state.
                        vlmData.setDiscGran(VlmProviderObject.UNINITIALIZED_SIZE);
                    }
                    else
                    {
                        failedMissingDecryptedPassphrase = true;
                        vlmData.setFailed(true);
                    }
                }
                else
                {
                    warnOnAllowDiscardsMismatch(vlmData, identifier, apiCallRc);
                }

                @Nullable byte[] expectedEncrPw = getExpectedEncryptedLuksPassword(vlmData);
                if (expectedEncrPw != null && !Arrays.equals(expectedEncrPw, vlmData.getEncryptedKey()))
                {
                    // if modify password is set, we are supposed to change the luks password to it.
                    byte[] masterKey = secObjs.getCryptKey();
                    try
                    {
                        byte[] plainModifyPassword = decryptionHelper.decrypt(masterKey, expectedEncrPw);
                        cryptSetup.changeKey(
                            vlmData.getDataDevice(), vlmData.getDecryptedPassword(), plainModifyPassword
                        );

                        vlmData.setEncryptedKey(expectedEncrPw);
                        vlmData.setDecryptedPassword(plainModifyPassword);
                    }
                    catch (LinStorException exc)
                    {
                        throw new StorageException("Unable to decrypt modify password key.", exc);
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
                vlmData.setExists(true);
                vlmData.setFailed(false);
            }
            if (!deleteVlm && vlmData.hasCorruptedKey())
            {
                vlmsWithCorruptKey.add(vlm);
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
        if (!vlmsWithCorruptKey.isEmpty())
        {
            boolean plural = vlmsWithCorruptKey.size() > 1;
            String msg = "Volume" + (plural ? "s " : " ") + "of resource " + rscData.getResourceName().displayValue +
                (plural ? " have " : " has ") + "corrupted LUKS key. Resource is still usable but cannot be re-opened!";
            apiCallRc.add(ApiCallRcImpl.simpleEntry(ApiConsts.WARN_CORRUPT_CRYPT_KEY, msg));
        }
    }

    /**
     * Returns the base64-decoded value of the volume-definition's "Encryption/Passphrase" property,
     * or {@code null} if the property is not set. If the returned (non-null) value differs from
     * {@link LuksVlmData#getEncryptedKey()}, the LUKS key on disk needs to be updated to match.
     */
    private @Nullable byte[] getExpectedEncryptedLuksPassword(LuksVlmData<Resource> vlmDataRef)
    {
        @Nullable byte[] ret = null;
        try
        {
            @Nullable String propVal = vlmDataRef.getVolume().getVolumeDefinition().getProps(sysCtx)
                .getProp(ApiConsts.KEY_PASSPHRASE, ApiConsts.NAMESPC_ENCRYPTION);
            if (propVal != null)
            {
                ret = Base64.decode(propVal);
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return ret;
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
                "%s_%05d",
                vlmData.getRscLayerObject().getSuffixedResourceName(),
                vlmData.getVlmNr().value
            );
            vlmData.setIdentifier(identifier);
        }

        return identifier;
    }

    private void warnOnAllowDiscardsMismatch(
        LuksVlmData<Resource> vlmData,
        String identifier,
        ApiCallRcImpl apiCallRc
    )
        throws StorageException
    {
        boolean wanted = isAllowDiscardsEnabled(vlmData);
        boolean live = cryptSetup.getActiveFlags(identifier).contains("discards");
        if (wanted != live)
        {
            String msg = String.format(
                "LUKS device '%s' is already open with a different discard setting " +
                    "(wanted: %s, active: %s). Deactivate and reactivate the resource " +
                    "for %s/%s to take effect.",
                vlmData.getDevicePath(),
                wanted,
                live,
                ApiConsts.NAMESPC_LUKS,
                ApiConsts.KEY_LUKS_ALLOW_DISCARDS
            );
            errorReporter.logWarning(msg);
            apiCallRc.addEntry(ApiCallRcImpl.simpleEntry(ApiConsts.MASK_WARN | ApiConsts.MASK_VLM, msg));
        }
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
                List<String> createOptions = getCreateOptions(luksVlmData);

                // initialize luks device
                cryptSetup.createLuksDevice(
                    storageChild.getCloneDevicePath(),
                    luksVlmData.getDecryptedPassword(),
                    identifier,
                    createOptions
                );
            }

            cryptSetup.openLuksDevice(
                storageChild.getCloneDevicePath(),
                identifier,
                luksVlmData.getDecryptedPassword(),
                targetRscNameRef != null,
                getOpenOptions(luksVlmData)
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

    @Override
    public DeviceLayerKind getKind()
    {
        return DeviceLayerKind.LUKS;
    }
    public List<String> getCreateOptions(final LuksVlmData<Resource> vlmData)
    {
        return getOptions(vlmData, ApiConsts.KEY_STOR_DRIVER_LUKS_FORMAT_OPTIONS);
    }

    public List<String> getOpenOptions(final LuksVlmData<Resource> vlmData)
    {
        List<String> opts = getOptions(vlmData, ApiConsts.KEY_STOR_DRIVER_LUKS_OPEN_OPTIONS);
        if (isAllowDiscardsEnabled(vlmData) && !opts.contains("--allow-discards"))
        {
            opts.add("--allow-discards");
        }
        return opts;
    }

    public List<String> getOptions(final LuksVlmData<Resource> vlmData, final String optionsKey)
    {
        try
        {
            PriorityProps prioProps = buildPriorityProps(vlmData);
            final @Nullable String userOptProp = prioProps.getProp(optionsKey, STOR_DRIVER_NAMESPACE);
            return userOptProp != null ? ShellUtils.shellSplit(userOptProp) : new ArrayList<>();
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    public boolean isAllowDiscardsEnabled(final LuksVlmData<Resource> vlmData)
    {
        try
        {
            PriorityProps prioProps = buildPriorityProps(vlmData);
            final @Nullable String val = prioProps.getProp(
                ApiConsts.KEY_LUKS_ALLOW_DISCARDS,
                ApiConsts.NAMESPC_LUKS
            );
            return ApiConsts.VAL_TRUE.equalsIgnoreCase(val);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    private PriorityProps buildPriorityProps(final LuksVlmData<Resource> vlmData)
        throws AccessDeniedException
    {
        Volume vlm = (Volume) vlmData.getVolume();
        Resource rsc = vlm.getAbsResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();

        PriorityProps prioProps = new PriorityProps(
            vlm.getProps(sysCtx),
            rsc.getProps(sysCtx)
        );
        for (StorPool storPool : LayerVlmUtils.getStorPoolSet(vlmData, sysCtx))
        {
            prioProps.addProps(storPool.getProps(sysCtx));
        }
        prioProps.addProps(
            rsc.getNode().getProps(sysCtx),
            vlmDfn.getProps(sysCtx),
            rscDfn.getProps(sysCtx),
            rscGrp.getVolumeGroupProps(sysCtx, vlmDfn.getVolumeNumber()),
            rscGrp.getProps(sysCtx),
            stltConfigAccessor.getReadonlyProps()
        );
        return prioProps;
    }

    private boolean allStorageChildrenZfs(LuksRscData<Resource> luksRscDataRef)
    {
        boolean allZfs = true;
        List<AbsRscLayerObject<Resource>> storageRscList =
            LayerUtils.getChildLayerDataByKind(luksRscDataRef, DeviceLayerKind.STORAGE);
        for (AbsRscLayerObject<Resource> storageRsc : storageRscList)
        {
            for (VlmProviderObject<Resource> vlm : storageRsc.getVlmLayerObjects().values())
            {
                DeviceProviderKind kind = vlm.getProviderKind();
                if (kind != DeviceProviderKind.ZFS && kind != DeviceProviderKind.ZFS_THIN)
                {
                    allZfs = false;
                }
            }
        }
        return allZfs;
    }
}
