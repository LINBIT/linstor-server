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
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestControllers.ExosRestPort;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestInitiators;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestInitiators.ExosRestInitiator;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeView;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeViewMapping;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPoolCollection.ExosRestPool;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPorts;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestVolumesCollection.ExosRestVolume;
import com.linbit.linstor.layer.storage.utils.FsUtils;
import com.linbit.linstor.layer.storage.utils.LsscsiUtils;
import com.linbit.linstor.layer.storage.utils.LsscsiUtils.LsscsiRow;
import com.linbit.linstor.layer.storage.utils.MkfsUtils;
import com.linbit.linstor.layer.storage.utils.SysClassUtils;
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
import com.linbit.linstor.storage.utils.ExosMappingManager;
import com.linbit.linstor.transaction.manager.TransactionMgr;
import com.linbit.utils.Align;
import com.linbit.utils.ExceptionThrowingConsumer;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class ExosProvider extends AbsStorageProvider<ExosRestVolume, ExosData<Resource>, ExosData<Snapshot>>
{
    public static final String EXOS_POOL_NAME = InternalApiConsts.NAMESPC_EXOS + "/PoolName";
    public static final String EXOS_POOL_SERIAL_NUMBER =
        ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.KEY_STOR_POOL_EXOS_POOL_SN;

    private static final int MAX_LSSCSI_RETRY_COUNT = 50;

    private static final String ALL_OTHER_INITIATORS = "all other initiators";

    private static final int KiB = 1024;
    // linstor calculates everything in KiB.. 1<<12 is therefore 4MiB outside of linstor
    private static final Align ALIGN_TO_NEXT_4MB = new Align(1L << 12);
    private static final long SECTORS_PER_KIB = KiB / 512;

    private static final int TOLERANCE_FACTOR = 3;
    public static final String FORMAT_RSC_TO_LVM_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_LVM_ID = "%s%s_%s_%05d";

    private static final Pattern HCTL_PATTERN = Pattern.compile("\\[([0-9]+):([0-9]+):([0-9]+):([0-9]+)\\]");

    private final ExosRestClient restClient;

    private Set<String> exosInitiatorIds;
    // private Map<String, String> exosCtrlMacAddrMapById;
    private Map<String, String> exosCtrlNameMapByTargetId;
    private Set<String> enclosureHostIds;
    /**
     * Stores the exos-internal pool name (usually "A" or "B").
     * The exos pool is found by the linstor-property exos_pool_sn (serial number)
     */
    private final Map<StorPool, String> exosPoolNameMap = new HashMap<>();

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

        exosInitiatorIds = new HashSet<>();
        // exosCtrlMacAddrMapById = new HashMap<>();
        exosCtrlNameMapByTargetId = new HashMap<>();
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
        HashMap<StorPool, String> storPoolWithNames = new HashMap<>();
        HashSet<StorPool> affectedStorPools = getAffectedStorPools(vlmDataList, snapVlms);
        for (StorPool sp : affectedStorPools)
        {
            storPoolWithNames.put(sp, getExosPoolName(sp));
        }
        return restClient.getVlmInfo(
            this::asIdentifier,
            storPoolWithNames
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

        StorPool storPool = vlmData.getStorPool();
        ExosRestVolume exosVlm = restClient.createVolume(
            storPool,
            getExosPoolName(storPool),
            vlmData.getShortName(),
            ALIGN_TO_NEXT_4MB.ceiling(vlmData.getExpectedSize()),
            additionalOptions
        );
        // only look for lsscsi devices AFTER the mapping is done
        updateFromLsscsi(vlmData, exosVlm, allLssciRowsPreCreate);
    }

    /**
     * Exos default mapping is not good, so we need to unmap (only the default)
     * and afterwards map us (as in Exos initiator) to the just created volume (maybe "additionally")
     *
     * @param vlmDataRef
     *
     * @throws AccessDeniedException
     * @throws StorageException
     */
    private void handleMapping(ExosRestVolume exosVlmRef, ExosData<?> vlmDataRef)
        throws StorageException, AccessDeniedException
    {
        String exosVlmName = vlmDataRef.getShortName();

        // first, check the mapping to see if we need to unmap the default
        ExosVolumeView exosVolumeView = restClient.showMaps(exosVlmName);

        if (exosVolumeView == null) {
            throw new StorageException("'show maps " + exosVlmName + "' returned no result");
        }

        final List<Triple<String, String, String>> expectedMappingListOrig = Collections.unmodifiableList(
            ExosMappingManager.getCtrlnamePortLunList(
                vlmDataRef.getVolume().getProps(storDriverAccCtx)
            )
        );

        if (exosVolumeView.volumeViewMappings != null)
        {
            for (ExosVolumeViewMapping mapping : exosVolumeView.volumeViewMappings)
            {
                if (mapping.identifier.equals(ALL_OTHER_INITIATORS) && mapping.accessNumeric != 0)
                {
                    // release default mapping
                    restClient.unmap(exosVlmName, null);
                    break;
                }
            }
        }

        // ensure exosInitiators are mapped as expected.
        // release unexpected mappings and
        // create expected mappings if the not already exist.
        for (String exosInitiatorId : exosInitiatorIds)
        {
            List<Triple<String, String, String>> expectedMappingListCopy = new ArrayList<>(expectedMappingListOrig);
            boolean hasUnexpectedMapping = false;
            if (exosVolumeView.volumeViewMappings != null)
            {
                for (ExosVolumeViewMapping mapping : exosVolumeView.volumeViewMappings)
                {
                    if (exosInitiatorId.equals(mapping.identifier))
                    {
                        if (mapping.accessNumeric != 0)
                        {
                            String lun = mapping.lun;
                            String portsRaw = mapping.ports;
                            for (String ctrlPort : portsRaw.split(","))
                            {
                                String ctrlName = ctrlPort.substring(0, ctrlPort.length() - 1);
                                String port = ctrlPort.substring(ctrlPort.length() - 1);

                                Triple<String, String, String> cpl = new Triple<>(ctrlName, port, lun);
                                if (!expectedMappingListCopy.remove(cpl))
                                {
                                    hasUnexpectedMapping = true;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if (hasUnexpectedMapping || !expectedMappingListCopy.isEmpty())
            {
                // do not use the *Copy list for the next call as we want to include ALL mappings, not just the missing
                Map<String, String> mappingPairMap = groupCtrlPortPairsByLun(expectedMappingListOrig);
                for (Entry<String, String> mappingPair : mappingPairMap.entrySet())
                {
                    restClient.map(
                        exosVlmName,
                        mappingPair.getKey(),
                        mappingPair.getValue(),
                        Collections.singletonList(exosInitiatorId)
                    );
                }
            }
        }
    }


    /**
     * Pairs port with same lun.
     * Example:
     * input <"A", "0", "1">, <"A", "1", "1">, <"A", "2", "2">, <"B", "0", "1">
     * output: <"A0,A1,B0", "1">, <"A2", "2">
     *
     * @param expectedMappingListCopyRef
     *
     * @return
     */
    private static Map<String, String> groupCtrlPortPairsByLun(
        List<Triple<String, String, String>> expectedMappingListCopyRef
    )
    {
        Map<String, String> ctrlPortsByLun = new HashMap<>();
        for (Triple<String, String, String> entry : expectedMappingListCopyRef)
        {
            String ctrlPorts = ctrlPortsByLun.get(entry.objC);
            if (ctrlPorts == null)
            {
                ctrlPorts = entry.objA + entry.objB;
            }
            else
            {
                ctrlPorts += "," + entry.objA + entry.objB;
            }
            ctrlPortsByLun.put(entry.objC, ctrlPorts);
        }
        return ctrlPortsByLun;
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
        handleMapping(exosVlm, vlmData);

        String devicePathToUse = null;
        ArrayList<String> hctlList = null;

        int retry = 0;
        while (retry++ < MAX_LSSCSI_RETRY_COUNT)
        {
            LsscsiUtils.rescan(extCmdFactory, errorReporter, enclosureHostIds);
            List<LsscsiRow> newLsscsiRowEntries = LsscsiUtils.getAll(extCmdFactory);
            if (lsscsiRowsToIgnore != null)
            {
                newLsscsiRowEntries.removeAll(lsscsiRowsToIgnore);
            }
            String wwn = exosVlm.wwn.toUpperCase();
            // String preferredMac = exosCtrlMacAddrMapById.get(exosVlm.owner);

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
                Path scsiDevicePath = Paths.get("/sys/class/scsi_device/").resolve(lsscsiRow.hctl).resolve("device");
                String scsiIdentSerial = FsUtils.readAllBytes(scsiDevicePath.resolve("wwid")).trim();

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
                        errorReporter.logTrace(
                            "Adding HCTL [%s] to volume %s",
                            lsscsiRow.hctl,
                            vlmData.getIdentifier()
                        );
                        hctlList.add(lsscsiRow.hctl);

                        if (devicePathToUse == null && devPath != null)
                        {
                            devicePathToUse = devPath; // use the first if the preferred controller was not found
                        }

                        String sasAddress = FsUtils.readAllBytes(scsiDevicePath.resolve("sas_address")).trim();

                        // String mac = SdparmUtils.getMac(extCmdFactory, devPath).replaceAll(" ", ":");
                        // if (mac.equalsIgnoreCase(preferredMac))
                        if (exosVlm.owner.equals(exosCtrlNameMapByTargetId.get(sasAddress)))
                        {
                            devicePathToUse = devPath;
                            errorReporter.logTrace("Device %s matches preferred controller. Choosing.", devPath);
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

    private String getExosPoolName(StorPool storPoolRef)
        throws InvalidKeyException, StorageException, AccessDeniedException
    {
        String exosPoolName = exosPoolNameMap.get(storPoolRef);
        if (exosPoolName == null)
        {
            exosPoolName = restClient.getPool(storPoolRef).name;
            exosPoolNameMap.put(storPoolRef, exosPoolName);
        }
        return exosPoolName;
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
    public void setLocalNodeProps(Props localNodePropsRef) throws StorageException, AccessDeniedException
    {
        Set<String> newEnclosureHostIds = new HashSet<>();
        List<LsscsiRow> allRows = LsscsiUtils.getAll(extCmdFactory);
        for (LsscsiRow row : allRows)
        {
            if (row.type.equalsIgnoreCase("enclosu"))
            {
                Matcher matcher = HCTL_PATTERN.matcher(row.hctl);
                if (!matcher.find())
                {
                    throw new StorageException("Failed to parse HCTL: " + row.hctl);
                }
                newEnclosureHostIds.add(matcher.group(1));
            }
        }
        enclosureHostIds = newEnclosureHostIds;

        // update initiator-ids...
        Set<String> localScsiInitiatorIds = SysClassUtils.getScsiInitiatorIds(extCmdFactory);

        // filter localScsi*Ids for ids that Exos also knows
        ExosRestInitiators exosInitiators = restClient.showInitiators();

        Set<String> exosScsiInitiatorIds = new HashSet<>();
        for (ExosRestInitiator exosInitiator : exosInitiators.initiator)
        {
            exosScsiInitiatorIds.add(exosInitiator.id);
        }
        localScsiInitiatorIds.retainAll(exosScsiInitiatorIds);
        exosInitiatorIds = localScsiInitiatorIds;


        // recache controller[].port[].target-id -> controller map
        Set<String> localScsiTargetIds = SysClassUtils.getScsiTargetIds(extCmdFactory);
        Map<String, String> exosCtrlNameMapByTargetIdNew = new HashMap<>();
        ExosRestPorts exosRestPorts = restClient.showPorts();
        for (ExosRestPort exosRestPort : exosRestPorts.port)
        {
            if (localScsiTargetIds.contains(exosRestPort.targetId))
            {
                localNodePropsRef.setProp(
                    ApiConsts.NAMESPC_EXOS + "/" + enclosureName + "/" + ctrlName + "/Ports/" + ep.id,
                    ExosMappingManager.CONNECTED
                );
                exosCtrlNameMapByTargetIdNew.put(exosRestPort.targetId, exosRestPort.controller);
            }
        }
        exosCtrlNameMapByTargetId = exosCtrlNameMapByTargetIdNew;

        Set<String> exosScsiTargetIds = new HashSet<>();
        for (ExosRestPort exosPort : exosPorts.port)
        {
            exosScsiTargetIds.add(exosPort.targetId);
        }
        localScsiTargetIds.retainAll(exosScsiTargetIds);
        exosTargetIds = localScsiTargetIds;
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

        String ids = localNodeProps.getProp(
            ApiConsts.KEY_STOR_POOL_EXOS_INITIATOR_IDS,
            ApiConsts.NAMESPC_STORAGE_DRIVER
        );
        if (ids == null)
        {
            throw new StorageException(
                ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.KEY_STOR_POOL_EXOS_INITIATOR_IDS + " must be set"
            );
        }
        PriorityProps prioProps = new PriorityProps(localNodeProps, stltConfigAccessor.getReadonlyProps());
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