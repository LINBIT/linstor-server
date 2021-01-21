package com.linbit.linstor.layer.storage.exos;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.exos.rest.ExosRestClient;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestControllers;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestControllers.ExosRestController;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeView;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeViewMapping;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPoolCollection.ExosRestPool;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestVolumesCollection.ExosRestVolume;
import com.linbit.linstor.layer.storage.utils.FsUtils;
import com.linbit.linstor.layer.storage.utils.LsscsiUtils;
import com.linbit.linstor.layer.storage.utils.LsscsiUtils.LsscsiRow;
import com.linbit.linstor.layer.storage.utils.MkfsUtils;
import com.linbit.linstor.layer.storage.utils.SdparmUtils;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.exos.ExosData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Align;
import com.linbit.utils.ExceptionThrowingConsumer;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Singleton
public class ExosProvider extends AbsStorageProvider<ExosRestVolume, ExosData<Resource>, ExosData<Snapshot>>
{
    private static final int MAX_LSSCSI_RETRY_COUNT = 50;

    private static final String ALL_OTHER_INITIATORS = "all other initiators";

    private static final int KiB = 1024;
    // linstor calculates everything in KiB.. 1<<12 is therefore 4MiB outside of linstor
    private static final Align ALIGN_TO_NEXT_4MB = new Align(1L << 12);
    private static final long SECTORS_PER_KIB = KiB / 512;

    private static final int TOLERANCE_FACTOR = 3;
    public static final String FORMAT_RSC_TO_LVM_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_LVM_ID = "%s%s_%s_%05d";

    private final ExosRestClient restClient;

    private List<String> exosInitiatorIds;
    private Map<String, String> exosCtrlMacAddrMapById;

    @Inject
    public ExosProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        ExosRestClient restClientRef
    )
    {
        super(
            errorReporter,
            extCmdFactory,
            storDriverAccCtx,
            stltConfigAccessor,
            wipeHandler,
            notificationListenerProvider,
            transMgrProvider,
            "EXOS",
            DeviceProviderKind.EXOS,
            snapShipMrgRef,
            extToolsCheckerRef
        );
        restClient = restClientRef;

        exosInitiatorIds = new ArrayList<>();
        exosCtrlMacAddrMapById = new HashMap<>();
    }

    @Override
    public void clearCache() throws StorageException
    {
        super.clearCache();
        LsscsiUtils.clearCache();
    }

    @Override
    protected Map<String, ExosRestVolume> getInfoListImpl(
        List<ExosData<Resource>> vlmDataList,
        List<ExosData<Snapshot>> snapVlms
    )
        throws StorageException, AccessDeniedException
    {
        return restClient.getVlmInfo(
            this::asIdentifier,
            getAffectedStorPools(vlmDataList, snapVlms)
        );
    }

    private String asIdentifier(StorPool sp, ExosRestVolume exosVlm)
    {
        return sp.getSharedStorPoolName().displayValue + "/" + exosVlm.volumeName;
    }

    private String asIdentifierRaw(ExosData<?> exosData)
    {
        return exosData.getStorPool().getSharedStorPoolName().displayValue + "/" + exosData.getShortName();
    }

    @Override
    protected void updateStates(List<ExosData<Resource>> vlmDataList, List<ExosData<Snapshot>> snapVlmDataList)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        List<ExosData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlmDataList);

        for (ExosData<?> vlmData : combinedList)
        {
            final ExosRestVolume exosVlm = infoListCache.get(asIdentifierRaw(vlmData));
            updateInfo(vlmData, exosVlm);

            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            if (exosVlm != null)
            {
                final long expectedSize = vlmData.getExpectedSize();
                final long actualSize = exosVlm.sizeNumeric / SECTORS_PER_KIB;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        vlmData.setSizeState(Size.TOO_SMALL);
                    }
                    else
                    {
                        Size sizeState = Size.TOO_LARGE;

                        final long toleratedSize =
                            expectedSize + 4 * 1024 * TOLERANCE_FACTOR;
                        if (actualSize < toleratedSize)
                        {
                            sizeState = Size.TOO_LARGE_WITHIN_TOLERANCE;
                        }
                        vlmData.setSizeState(sizeState);
                    }
                }
                else
                {
                    vlmData.setSizeState(Size.AS_EXPECTED);
                }
            }
        }
    }

    /*
     * Expected to be overridden (extended) by LvmThinProvider
     */
    protected void updateInfo(ExosData<?> vlmDataRef, ExosRestVolume exosVlm)
        throws DatabaseException, AccessDeniedException, StorageException
    {
        if (exosVlm == null)
        {
            vlmDataRef.setExists(false);
            vlmDataRef.setDevicePath(null);
            vlmDataRef.setAllocatedSize(-1);
            // vlmData.setUsableSize(-1);
            vlmDataRef.setDevicePath(null);
        }
        else
        {
            vlmDataRef.setExists(true);
            handleMapping(exosVlm, vlmDataRef.getIdentifier(), getLun(vlmDataRef));
            updateFromLsscsi(vlmDataRef, exosVlm, null);
        }
    }

    @Override
    protected void createLvImpl(ExosData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        List<LsscsiRow> allLssciRowsPreCreate = LsscsiUtils.getAll(extCmdFactory);

        List<String> additionalOptions = MkfsUtils.shellSplit(getCreateVlmOptions(vlmData));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        String exosVlmName = vlmData.getShortName();

        ExosRestVolume exosVlm = restClient.createVolume(
            vlmData.getStorPool(),
            exosVlmName,
            ALIGN_TO_NEXT_4MB.ceiling(vlmData.getExpectedSize()),
            additionalOptions
        );

        handleMapping(exosVlm, exosVlmName, getLun(vlmData));

        // only look for lsscsi devices AFTER the mapping is done
        updateFromLsscsi(vlmData, exosVlm, allLssciRowsPreCreate);
    }

    /**
     * Exos default mapping is not good, so we need to unmap (only the default)
     * and afterwards map us (as in Exos initiator) to the just created volume (maybe "additionally")
     *
     * @throws AccessDeniedException
     * @throws StorageException
     */
    private void handleMapping(ExosRestVolume exosVlmRef, String exosVlmNameRef, int lun)
        throws StorageException, AccessDeniedException
    {
        // first, check the mapping to see if we need to unmap the default
        ExosVolumeView exosVolumeView = restClient.showMaps(exosVlmNameRef);

        if (exosVolumeView == null) {
            throw new StorageException("'show maps " + exosVlmNameRef + "' returned no result");
        }

        boolean hasDefaultMapping = false;
        String lunFromSpecialMapping = null;
        if (exosVolumeView.volumeViewMappings != null)
        {
            for (ExosVolumeViewMapping mapping : exosVolumeView.volumeViewMappings)
            {
                if (mapping.identifier.equals(ALL_OTHER_INITIATORS))
                {
                    if (mapping.accessNumeric != 0)
                    {
                        hasDefaultMapping = true;
                    }
                }
                else if (exosInitiatorIds.contains(mapping.identifier))
                {
                    if (mapping.accessNumeric != 0)
                    {
                        lunFromSpecialMapping = mapping.lun;
                        break;
                    }
                }
            }
        }

        if (hasDefaultMapping)
        {
            restClient.unmap(exosVlmNameRef, null);
        }
        if (lunFromSpecialMapping == null)
        {
            restClient.map(exosVlmNameRef, lun, exosInitiatorIds);
        }
    }

    /**
     * Fetches <code>lsscsi -w</code> and sets the <code>exosVlm</code>'s device path, the HCTL entries and the usable
     * and allocated sizes
     *
     * @param vlmData
     * @param exosVlm
     * @param lsscsiRowsToIgnore
     *
     * @throws StorageException
     * @throws InvalidKeyException
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    private void updateFromLsscsi(
        ExosData<?> vlmData,
        ExosRestVolume exosVlm,
        List<LsscsiRow> lsscsiRowsToIgnore
    )
        throws StorageException, InvalidKeyException, AccessDeniedException, DatabaseException
    {
        String devicePathToUse = null;
        ArrayList<String> hctlList = null;

        int retry = 0;
        while (retry++ < MAX_LSSCSI_RETRY_COUNT)
        {

            LsscsiUtils.rescan(extCmdFactory, errorReporter, getLun(vlmData));
            List<LsscsiRow> newLsscsiRowEntries = LsscsiUtils.getAll(extCmdFactory);
            if (lsscsiRowsToIgnore != null)
            {
                newLsscsiRowEntries.removeAll(lsscsiRowsToIgnore);
            }
            String wwn = exosVlm.wwn.toUpperCase();
            String preferredMac = exosCtrlMacAddrMapById.get(exosVlm.owner);

            boolean allGood = true;
            hctlList = new ArrayList<>();
            for (LsscsiRow lsscsiRow : newLsscsiRowEntries)
            {
                /*
                 * scsiIdentSerial will be something like
                 * naa.600c0ff00029a5f56e7bfe5f01000000
                 *
                 * whereas wwn is only
                 * 600C0FF00029A5F56E7BFE5F01000000
                 */
                String scsiIdentSerial = FsUtils.readAllBytes(
                    Paths.get("/sys/class/scsi_device/" + lsscsiRow.hctl + "/device/wwid")
                ).trim();

                if (scsiIdentSerial.toUpperCase().endsWith(wwn))
                {
                    String devPath = lsscsiRow.devPath;
                    if (devPath.trim().equals("-"))
                    {
                        errorReporter.logTrace("Device path '%s' is invalid. retrying lsscsi", devPath);
                        allGood = false;
                        if (retry != MAX_LSSCSI_RETRY_COUNT)
                        {
                            // this is the NOT last run before "giving up" and fallback to "best effort"
                            break;
                        }
                    }
                    else
                    {
                        errorReporter.logTrace("Device %s matches WWN for newly created device", devPath);

                        if (devicePathToUse == null && devPath != null)
                        {
                            devicePathToUse = devPath; // use the first if preferredMac was not found
                        }

                        errorReporter.logTrace(
                            "Adding HCTL [%s] to volume %s",
                            lsscsiRow.hctl,
                            vlmData.getIdentifier()
                        );
                        hctlList.add(lsscsiRow.hctl);

                        String mac = SdparmUtils.getMac(extCmdFactory, devPath).replaceAll(" ", ":");
                        if (mac.equalsIgnoreCase(preferredMac))
                        {
                            devicePathToUse = devPath;
                            errorReporter.logTrace("Device %s matches preferred MAC address. Choosing", devPath);
                        }
                    }
                }
            }
            if (allGood)
            {
                break;
            }
            else
            {
                try
                {
                    Thread.sleep(100);
                }
                catch (InterruptedException ignored)
                {}
                // and retry
            }
        }
        vlmData.setHCTL(hctlList);

        vlmData.setDevicePath(devicePathToUse);
        vlmData.setAllocatedSize(exosVlm.sizeNumeric / SECTORS_PER_KIB);
        vlmData.setUsableSize(exosVlm.sizeNumeric / SECTORS_PER_KIB);
    }

    private List<String> getHCTL(ExosData<Resource> vlmData)
        throws StorageException, InvalidKeyException, AccessDeniedException
    {
        List<LsscsiRow> lsscsiRowByLun = LsscsiUtils.getLsscsiRowByLun(extCmdFactory, getLun(vlmData));
        List<String> hctlList = new ArrayList<>();
        for (LsscsiRow row : lsscsiRowByLun)
        {
            hctlList.add(row.hctl);
        }
        return hctlList;
    }

    private int getLun(ExosData<?> vlmDataRef)
        throws InvalidKeyException, AccessDeniedException
    {
        // TODO the LUN should be in a dedicated ExosVlmData
        return Integer.parseInt(
            vlmDataRef.getVolume().getProps(storDriverAccCtx).getProp(InternalApiConsts.EXOS_LUN)
        );
    }

    protected PriorityProps getPrioProps(ExosData<Resource> vlmDataRef) throws AccessDeniedException
    {
        Volume vlm = (Volume) vlmDataRef.getVolume();
        Resource rsc = vlm.getAbsResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        return new PriorityProps(
            vlm.getProps(storDriverAccCtx),
            rsc.getProps(storDriverAccCtx),
            vlmDataRef.getStorPool().getProps(storDriverAccCtx),
            localNodeProps,
            vlmDfn.getProps(storDriverAccCtx),
            rscDfn.getProps(storDriverAccCtx),
            rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmDfn.getVolumeNumber()),
            rscGrp.getProps(storDriverAccCtx),
            stltConfigAccessor.getReadonlyProps()
        );
    }

    protected String getCreateVlmOptions(ExosData<Resource> vlmDataRef)
    {
        String options;
        try
        {
            options = getPrioProps(vlmDataRef).getProp(
                ApiConsts.KEY_STOR_POOL_EXOS_CREATE_VOLUME_OPTIONS,
                ApiConsts.NAMESPC_STORAGE_DRIVER,
                ""
            );
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return options;
    }

    @Override
    protected void resizeLvImpl(ExosData<Resource> vlmData)
        throws StorageException, AccessDeniedException
    {
        final long additionalSizeInKib = vlmData.getExpectedSize() - vlmData.getAllocatedSize();
        restClient.expandVolume(vlmData.getStorPool(), vlmData.getShortName(), additionalSizeInKib);
    }

    @Override
    protected void deleteLvImpl(ExosData<Resource> vlmData, String oldLvmId)
        throws StorageException, DatabaseException, AccessDeniedException
    {
        String devicePath = vlmData.getDevicePath();

        if (devicePath != null)
        {
            wipeHandler.quickWipe(devicePath);
        }
        restClient.deleteVolume(vlmData.getStorPool(), vlmData.getShortName());
        try
        {
            for (String hctl : vlmData.getHCTLList())
            {
                errorReporter.logTrace("Deleting HCTL %s", hctl);
                Files.write(Paths.get("/sys/class/scsi_device/" + hctl + "/device/delete"), "1".getBytes());
            }
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to delete scsi_device. List of HCTLs: " + vlmData.getHCTLList(), exc);
        }
        vlmData.setExists(false);
    }

    @Override
    protected void deactivateLvImpl(ExosData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // noop
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSizes = new HashMap<>();
        try
        {
            for (StorPool sp : getChangedStorPools())
            {
                ExosRestPool respPool;
                respPool = restClient.getPool(sp);

                freeSizes.put(
                    sp.getProps(storDriverAccCtx).getProp(ApiConsts.KEY_STOR_POOL_NAME),
                    respPool.totalAvailNumeric / SECTORS_PER_KIB
                );
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new StorageException("Error occurred gathering free spaces", exc);
        }
        return freeSizes;
    }

    @Override
    protected String getDevicePath(String storageName, String lvId)
    {
        return null;
    }

    @Override
    protected String asLvIdentifier(ResourceName resourceName, String rscNameSuffix, VolumeNumber volumeNumber)
    {
        return String.format(
            FORMAT_RSC_TO_LVM_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String asSnapLvIdentifierRaw(String rscNameRef, String rscNameSuffixRef, String snapNameRef, int vlmNrRef)
    {
        return String.format(
            FORMAT_SNAP_TO_LVM_ID,
            rscNameRef,
            rscNameSuffixRef,
            vlmNrRef,
            snapNameRef
        );
    }

    @Override
    protected String getStorageName(StorPool storPoolRef)
    {
        return getPoolName(storPoolRef);
    }

    protected String getPoolName(StorPool storPool)
    {
        String poolName;
        try
        {
            poolName = DeviceLayerUtils.getNamespaceStorDriver(
                    storPool.getProps(storDriverAccCtx)
                )
                .getProp(ApiConsts.KEY_STOR_POOL_NAME);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return poolName;
    }

    @Override
    protected boolean updateDmStats()
    {
        return false;
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPool storPool) throws StorageException, AccessDeniedException
    {
        ExosRestPool exosPool = restClient.getPool(storPool);
        return new SpaceInfo(
            exosPool.totalSizeNumeric / SECTORS_PER_KIB,
            exosPool.totalAvailNumeric / SECTORS_PER_KIB
        );
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        Props props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getProps(storDriverAccCtx)
        );
        if (props.getProp(ApiConsts.KEY_STOR_POOL_NAME) == null)
        {
            throw new StorageException("Pool name must be set!");
        }
    }

    @Override
    public void update(StorPool storPoolRef) throws AccessDeniedException, DatabaseException, StorageException
    {
        // noop
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return true;
    }

    @Override
    protected void setDevicePath(ExosData<Resource> vlmData, String devPath) throws DatabaseException
    {
        // do not set device path as that will most likely be null. See comments in AbsStorageProvider#createVolumes
        // devicePath is set in updateFromLsscsi method
    }

    @Override
    protected void setAllocatedSize(ExosData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(ExosData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(ExosData<Resource> vlmData, long size)
    {
        vlmData.setExepectedSize(size);
    }

    @Override
    protected String getStorageName(ExosData<Resource> vlmDataRef) throws DatabaseException, AccessDeniedException
    {
        return vlmDataRef.getStorPool().getProps(storDriverAccCtx).getProp(
            ApiConsts.KEY_STOR_POOL_NAME,
            ApiConsts.NAMESPC_STORAGE_DRIVER
        );
    }

    @Override
    public void setLocalNodeProps(Props localNodePropsRef) throws StorageException, AccessDeniedException
    {
        super.setLocalNodeProps(localNodePropsRef);
        String ids = localNodePropsRef.getProp(
            ApiConsts.KEY_STOR_POOL_EXOS_INITIATOR_IDS,
            ApiConsts.NAMESPC_STORAGE_DRIVER
        );
        if (ids == null)
        {
            throw new StorageException(
                ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.KEY_STOR_POOL_EXOS_INITIATOR_IDS + " must be set"
            );
        }
        PriorityProps prioProps = new PriorityProps(localNodePropsRef, stltConfigAccessor.getReadonlyProps());
        ExceptionThrowingConsumer<String, StorageException> throwIfPropMissing = key ->
        {
            if (prioProps.getProp(key, ApiConsts.NAMESPC_STORAGE_DRIVER) == null)
            {
                throw new StorageException(key + " must be set");
            }
        };
        throwIfPropMissing.accept(ApiConsts.KEY_STOR_POOL_EXOS_API_HOST);
        throwIfPropMissing.accept(ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD);
        throwIfPropMissing.accept(ApiConsts.KEY_STOR_POOL_EXOS_API_USER);

        // recache controllerMac
        Map<String, String> exosCtrlMacAddrMapByIdNew = new HashMap<>();
        ExosRestControllers controllers = restClient.showControllers();
        for (ExosRestController exosCtrl : controllers.controllers)
        {
            exosCtrlMacAddrMapByIdNew.put(exosCtrl.controllerId, exosCtrl.macAddress);
        }
        exosCtrlMacAddrMapById = exosCtrlMacAddrMapByIdNew;

        exosInitiatorIds = new ArrayList<>(Arrays.asList(ids.split(",")));
    }

    private HashSet<StorPool> getAffectedStorPools(
        List<ExosData<Resource>> exosVlmDataList,
        List<ExosData<Snapshot>> exosSnapVlmDataList
    )
    {
        HashSet<StorPool> ret = new HashSet<>();
        for (ExosData<Resource> exosVlmData : exosVlmDataList)
        {
            ret.add(exosVlmData.getStorPool());
        }
        for (ExosData<Snapshot> exosSnapVlmData : exosSnapVlmDataList)
        {
            ret.add(exosSnapVlmData.getStorPool());
        }
        return ret;
    }
}
