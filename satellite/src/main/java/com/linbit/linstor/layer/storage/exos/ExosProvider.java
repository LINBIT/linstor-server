package com.linbit.linstor.layer.storage.exos;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.AbsVolume;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.exos.rest.ExosRestClient;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestControllers.ExosRestPort;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestInitiators;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestInitiators.ExosRestInitiator;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeView;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestMaps.ExosVolumeViewMapping;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPoolCollection.ExosRestPool;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestPorts;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestVolumesCollection;
import com.linbit.linstor.layer.storage.exos.rest.responses.ExosRestVolumesCollection.ExosRestVolume;
import com.linbit.linstor.layer.storage.utils.FsUtils;
import com.linbit.linstor.layer.storage.utils.LsscsiUtils;
import com.linbit.linstor.layer.storage.utils.LsscsiUtils.LsscsiRow;
import com.linbit.linstor.layer.storage.utils.MultipathUtils;
import com.linbit.linstor.layer.storage.utils.MultipathUtils.MultipathRow;
import com.linbit.linstor.layer.storage.utils.SysClassUtils;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.exos.ExosData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.utils.ExosMappingManager;
import com.linbit.linstor.storage.utils.MkfsUtils;
import com.linbit.utils.Align;
import com.linbit.utils.Triple;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;

@Deprecated(forRemoval = true)
@Singleton
public class ExosProvider extends AbsStorageProvider<ExosRestVolume, ExosData<Resource>, ExosData<Snapshot>>
{
    // linstor calculates everything in KiB.. 1<<12 is therefore 4MiB outside of linstor
    private static final long EXTENT_SIZE_IN_KIB = 1L << 12;

    private static final int MAX_LSSCSI_RETRY_COUNT = 10;

    private static final String ALL_OTHER_INITIATORS = "all other initiators";

    private static final int KiB = 1024;
    private static final Align ALIGN_TO_NEXT_4MB = new Align(EXTENT_SIZE_IN_KIB);
    private static final long SECTORS_PER_KIB = KiB / 512;

    private static final int TOLERANCE_FACTOR = 3;
    public static final String FORMAT_RSC_TO_LVM_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_LVM_ID = "%s%s_%s_%05d";

    private static final Pattern HCTL_PATTERN = Pattern.compile("\\[?([0-9]+):([0-9]+):([0-9]+):([0-9]+)\\]?");
    private static final long MASK_LSB_4 = 0xFFFFFFFFFFFFFFF0L; // ~15L

    private final Map<String, ExosRestClient> restClientMap;

    private Set<String> exosInitiatorIds;
    // private Map<String, String> exosCtrlMacAddrMapById;
    private Map<String, String> exosCtrlNameMapByTargetId;
    private @Nullable Set<String> enclosureHostIds;
    /**
     * Stores the exos-internal pool name (usually "A" or "B").
     * The exos pool is found by the linstor-property exos_pool_sn (serial number)
     */
    private final Map<StorPool, String> exosPoolNameMap = new HashMap<>();

    @Inject
    public ExosProvider(AbsStorageProviderInit superInitRef)
    {
        super(superInitRef, "EXOS", DeviceProviderKind.EXOS);
        restClientMap = new HashMap<>();

        exosInitiatorIds = new HashSet<>();
        // exosCtrlMacAddrMapById = new HashMap<>();
        exosCtrlNameMapByTargetId = new HashMap<>();
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.EXOS;
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
        HashMap<String, ExosRestVolume> ret = new HashMap<>();
        HashSet<StorPool> affectedStorPools = getAffectedStorPools(vlmDataList, snapVlms);

        HashMap<String, HashMap<StorPool, String>> storPoolsByEnclosure = new HashMap<>();
        for (StorPool sp : affectedStorPools)
        {
            String enclosureName = getEnclosureName(sp);
            HashMap<StorPool, String> storPoolWithNames = storPoolsByEnclosure.get(enclosureName);
            if (storPoolWithNames == null)
            {
                storPoolWithNames = new HashMap<>();
                storPoolsByEnclosure.put(enclosureName, storPoolWithNames);
            }
            storPoolWithNames.put(sp, getExosPoolName(sp));
        }

        for (Entry<String, HashMap<StorPool, String>> enclosureEntry : storPoolsByEnclosure.entrySet())
        {
            ExosRestClient restClient = getClient(enclosureEntry.getKey());
            HashMap<StorPool, ExosRestVolumesCollection> exosVolumeMap = restClient
                .getVolumes(enclosureEntry.getValue());

            for (Entry<StorPool, ExosRestVolumesCollection> entry : exosVolumeMap.entrySet())
            {
                StorPool storPool = entry.getKey();
                for (ExosRestVolume exosVlm : entry.getValue().volumes)
                {
                    ret.put(asIdentifier(storPool, exosVlm), exosVlm);
                }
            }
        }

        return ret;
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

    protected void updateInfo(ExosData<?> vlmDataRef, @Nullable ExosRestVolume exosVlm)
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
            updateFromLsscsi(vlmDataRef, exosVlm);
        }
    }

    @Override
    protected void createLvImpl(ExosData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        List<String> additionalOptions = MkfsUtils.shellSplit(getCreateVlmOptions(vlmData));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        StorPool storPool = vlmData.getStorPool();
        ExosRestVolume exosVlm = getClient(storPool).createVolume(
            storPool,
            getExosPoolName(storPool),
            vlmData.getShortName(),
            ALIGN_TO_NEXT_4MB.ceiling(vlmData.getExpectedSize()),
            additionalOptions
        );
        // only look for lsscsi devices AFTER the mapping is done
        updateFromLsscsi(vlmData, exosVlm);
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

        ExosRestClient restClient = getClient(vlmDataRef.getStorPool());

        // first, check the mapping to see if we need to unmap the default
        ExosVolumeView exosVolumeView = restClient.showMaps(exosVlmName);

        if (exosVolumeView == null)
        {
            throw new StorageException("'show maps " + exosVlmName + "' returned no result");
        }

        final List<Triple<String, String, String>> expectedMappingListOrig = Collections.unmodifiableList(
            ExosMappingManager.getCtrlnamePortLunList(
                getVlmProps(vlmDataRef)
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

        boolean isVlmActive = true;
        {
            AbsVolume<?> vlm = vlmDataRef.getVolume();
            if (vlm instanceof Volume)
            {
                isVlmActive = !((Volume) vlm).getAbsResource().getStateFlags()
                    .isSet(storDriverAccCtx, Resource.Flags.INACTIVE);
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
                            if (isVlmActive)
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
                            else
                            {
                                // we do not want this mapping
                                getClient(vlmDataRef.getStorPool()).unmap(
                                    vlmDataRef.getShortName(),
                                    Collections.singleton(exosInitiatorId)
                                );
                            }
                        }
                    }
                }
            }
            if (isVlmActive)
            {
                if (hasUnexpectedMapping || !expectedMappingListCopy.isEmpty())
                {
                    // do not use the *Copy list for the next call as we want to include ALL mappings, not just the
                    // missing
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
    }

    private ReadOnlyProps getVlmProps(ExosData<?> vlmDataRef) throws AccessDeniedException
    {
        ReadOnlyProps vlmRoProps;
        AbsVolume<?> absVlm = vlmDataRef.getVolume();
        if (absVlm instanceof Volume)
        {
            vlmRoProps = ((Volume) absVlm).getProps(storDriverAccCtx);
        }
        else
        {
            vlmRoProps = ((SnapshotVolume) absVlm).getVlmProps(storDriverAccCtx);
        }
        return vlmRoProps;
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
        ExosRestVolume exosVlm
    )
        throws StorageException, InvalidKeyException, AccessDeniedException, DatabaseException
    {
        handleMapping(exosVlm, vlmData);

        Set<String> lunsToInspect = getLunsToInspect(vlmData);
        String wwn = exosVlm.wwn.toUpperCase();
        errorReporter.logTrace("Luns to inspect: %s", lunsToInspect.toString());
        errorReporter.logTrace("Expected WWN: %s", wwn);

        String devicePathToUse = null;
        Set<String> hctlSet = null;

        boolean isVlmActive = true;
        {
            AbsVolume<?> absVolume = vlmData.getVolume();
            if (absVolume instanceof Volume)
            {
                Volume vlm = (Volume) absVolume;
                isVlmActive = !vlm.getAbsResource().getStateFlags().isSet(storDriverAccCtx, Resource.Flags.INACTIVE);
            }
        }
        int retry = isVlmActive ? 0 : MAX_LSSCSI_RETRY_COUNT - 1; // only scan once if vlm is expected to be inactive
        while (retry < MAX_LSSCSI_RETRY_COUNT)
        {
            ++retry;

            LsscsiUtils.rescan(extCmdFactory, errorReporter, enclosureHostIds);
            List<LsscsiRow> lsscsiRowEntries = LsscsiUtils.getAll(extCmdFactory);
            // String preferredMac = exosCtrlMacAddrMapById.get(exosVlm.owner);

            boolean lsscsiScanSuccess = true;
            hctlSet = new HashSet<>();
            for (LsscsiRow lsscsiRow : lsscsiRowEntries)
            {
                boolean inspect = false;
                for (String lunToInspect : lunsToInspect)
                {
                    if (lsscsiRow.hctl.endsWith(":" + lunToInspect))
                    {
                        inspect = true;
                        break;
                    }
                }

                if (inspect)
                {
                    lsscsiScanSuccess = inspectRow(lsscsiRow, wwn, vlmData, hctlSet);
                    if (!lsscsiScanSuccess && retry < MAX_LSSCSI_RETRY_COUNT)
                    {
                        // this is NOT the last run before "giving up" and fallback to "best effort"
                        break;
                    }
                }
            }

            String multiPathDevSuffix = null;
            if (lsscsiScanSuccess && !hctlSet.isEmpty())
            {
                List<MultipathRow> rows = MultipathUtils.getRowsByHCIL(extCmdFactory, hctlSet);
                if (rows.isEmpty())
                {
                    lsscsiScanSuccess = false;
                    if (retry >= MAX_LSSCSI_RETRY_COUNT)
                    {
                        throw new StorageException("Multipathd did not find given hctl: " + hctlSet);
                    }
                }
                for (MultipathRow row : rows)
                {
                    if (multiPathDevSuffix == null)
                    {
                        multiPathDevSuffix = row.multipathDev;
                    }
                    else
                    {
                        if (!multiPathDevSuffix.equals(row.multipathDev))
                        {
                            throw new StorageException(
                                "Multiple multipath-paths found: " + multiPathDevSuffix + ", " + row.multipathDev +
                                    ". Used HCTL list: " + hctlSet
                            );
                        }
                    }
                }
            }

            if (multiPathDevSuffix != null)
            {
                try
                {
                    Thread.sleep(1_000L); // give multipath a bit more time for setup
                }
                catch (InterruptedException ignored)
                {
                }

                devicePathToUse = "/dev/mapper/" + multiPathDevSuffix;
                errorReporter.logDebug("Found multipath device: %s", devicePathToUse);
                break;
            }
            else
            {
                try
                {
                    Thread.sleep(300L);
                }
                catch (InterruptedException ignored)
                {
                }
                // and retry
            }
        }
        if (isVlmActive)
        {
            if (devicePathToUse == null)
            {
                throw new StorageException("Failed to determine devicePath");
            }

            if (hctlSet != null && !hctlSet.isEmpty())
            {
                vlmData.setHCTL(hctlSet);
            }

            vlmData.setDevicePath(devicePathToUse);
            vlmData.setAllocatedSize(exosVlm.sizeNumeric / SECTORS_PER_KIB);
            vlmData.setUsableSize(exosVlm.sizeNumeric / SECTORS_PER_KIB);
        }
        else
        {
            if (hctlSet != null && !hctlSet.isEmpty())
            {
                // volume should be inactive. remove the local scsi devices
                // unmap should have happened in handleMapping method
                try
                {
                    for (String hctl : hctlSet)
                    {
                        errorReporter.logTrace("Deleting HCTL %s", hctl);
                        Files.write(Paths.get("/sys/class/scsi_device/" + hctl + "/device/delete"), "1".getBytes());
                    }
                    vlmData.setHCTL(Collections.emptySet());
                }
                catch (IOException exc)
                {
                    throw new StorageException(
                        "Failed to delete scsi_device. List of HCTLs: " + vlmData.getHCTLSet(),
                        exc
                    );
                }
            }
        }
    }

    private boolean inspectRow(
        final LsscsiRow     lsscsiRow,
        final String        wwn,
        final ExosData<?>   vlmData,
        final Set<String>   hctlSet
    )
        throws StorageException
    {
        boolean lsscsiScanSuccess = true;

        // scsiIdentSerial will be something like
        // naa.600c0ff00029a5f56e7bfe5f01000000
        //
        // whereas wwn is only
        // 600C0FF00029A5F56E7BFE5F01000000
        Path scsiDevicePath = Paths.get("/sys/class/scsi_device/").resolve(lsscsiRow.hctl).resolve("device");
        String scsiIdentSerial = FsUtils.readAllBytes(scsiDevicePath.resolve("wwid")).trim();

        errorReporter.logTrace("inspecting: %s found wwn: %s", lsscsiRow.hctl, scsiIdentSerial);

        if (scsiIdentSerial.toUpperCase().endsWith(wwn))
        {
            String devPath = lsscsiRow.devPath;
            if (devPath.trim().equals("-"))
            {
                errorReporter.logTrace("Device path '%s' is invalid. retrying lsscsi", devPath);
                lsscsiScanSuccess = false;
            }
            else
            {
                errorReporter.logTrace("Device %s matches WWN for newly created device", devPath);
                errorReporter.logTrace(
                    "Adding HCTL [%s] to volume %s",
                    lsscsiRow.hctl,
                    vlmData.getIdentifier()
                );
                hctlSet.add(lsscsiRow.hctl);
            }
        }
        return lsscsiScanSuccess;
    }

    private Set<String> getLunsToInspect(ExosData<?> vlmDataRef) throws AccessDeniedException
    {
        HashSet<String> ret = new HashSet<>();

        List<Triple<String, String, String>> ctrlnamePortLunList = ExosMappingManager.getCtrlnamePortLunList(
            getVlmProps(vlmDataRef)
        );

        for (Triple<String, String, String> triple : ctrlnamePortLunList)
        {
            ret.add(triple.objC);
        }

        return ret;
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
                ApiConsts.NAMESPC_EXOS,
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
        StorPool storPool = vlmData.getStorPool();
        getClient(storPool).expandVolume(storPool, vlmData.getShortName(), additionalSizeInKib);
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
        StorPool storPool = vlmData.getStorPool();
        getClient(storPool).deleteVolume(storPool, vlmData.getShortName());
        try
        {
            for (String hctl : vlmData.getHCTLSet())
            {
                errorReporter.logTrace("Deleting HCTL %s", hctl);
                Files.write(Paths.get("/sys/class/scsi_device/" + hctl + "/device/delete"), "1".getBytes());
            }
            vlmData.setHCTL(Collections.emptySet());
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to delete scsi_device. List of HCTLs: " + vlmData.getHCTLSet(), exc);
        }
        vlmData.setExists(false);
    }

    @Override
    protected void deactivateLvImpl(ExosData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // we need to unmap the exos volume and delete the local SCSI devices so that if this inactive
        // resource gets deleted, those mapping / local SCSI devices will not be left over

        StorPool storPool = vlmDataRef.getStorPool();
        getClient(storPool).unmap(vlmDataRef.getShortName(), exosInitiatorIds);

        try
        {
            for (String hctl : vlmDataRef.getHCTLSet())
            {
                errorReporter.logTrace("Deleting HCTL %s", hctl);
                Files.write(Paths.get("/sys/class/scsi_device/" + hctl + "/device/delete"), "1".getBytes());
            }
            vlmDataRef.setHCTL(Collections.emptySet());
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to delete scsi_device. List of HCTLs: " + vlmDataRef.getHCTLSet(), exc);
        }
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
                respPool = getClient(sp).getPool(sp);

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
    public @Nullable String getDevicePath(String storageName, String lvId)
    {
        return null;
    }

    @Override
    protected String asLvIdentifier(
        StorPoolName ignoredSpName,
        ResourceName resourceName,
        String rscNameSuffix,
        VolumeNumber volumeNumber
    )
    {
        return String.format(
            FORMAT_RSC_TO_LVM_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String asSnapLvIdentifier(ExosData<Snapshot> snapVlmDataRef)
    {
        StorageRscData<Snapshot> snapData = snapVlmDataRef.getRscLayerObject();
        return String.format(
            FORMAT_SNAP_TO_LVM_ID,
            snapData.getResourceName().displayValue,
            snapData.getResourceNameSuffix(),
            snapVlmDataRef.getVlmNr().value,
            snapData.getAbsResource().getSnapshotName().displayValue
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
            exosPoolName = getClient(storPoolRef).getPool(storPoolRef).name;
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
    public SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException, AccessDeniedException
    {
        ExosRestPool exosPool = getClient(storPool).getPool(storPool);
        return new SpaceInfo(
            exosPool.totalSizeNumeric / SECTORS_PER_KIB,
            exosPool.totalAvailNumeric / SECTORS_PER_KIB
        );
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(ReadOnlyProps localNodePropsRef)
        throws StorageException, AccessDeniedException
    {
        super.setLocalNodeProps(localNodePropsRef);

        return reinit();
    }

    private @Nullable LocalPropsChangePojo reinit() throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo ret = null;
        if (extToolsChecker.areSupported(false, ExtTools.LSSCSI, ExtTools.SAS_PHY, ExtTools.SAS_DEVICE))
        {
            initNewExosRestClients(stltConfigAccessor.getReadonlyProps());
            initNewExosRestClients(localNodeProps);

            for (ExosRestClient exosRestClient : restClientMap.values())
            {
                exosRestClient.setLocalNodeProps(localNodeProps);
            }

            reinitEnclosureHostIds();

            reinitInitiatorIds();
            ret = reinitTargetIds(localNodeProps);
        }
        return ret;
    }

    private void initNewExosRestClients(ReadOnlyProps props)
    {
        @Nullable ReadOnlyProps exosNamespace = props.getNamespace(ApiConsts.NAMESPC_EXOS);
        if (exosNamespace != null)
        {
            Iterator<String> exosNamespaceIt = exosNamespace.iterateNamespaces();

            while (exosNamespaceIt.hasNext())
            {
                String enclosureName = exosNamespaceIt.next();
                getClient(enclosureName);
            }
        }
    }

    /**
     * Repopulates the <code>enclosureHostIds</code> set which contains the host from the HCTL pair of the "enclosu"
     * entries from the output of <code>lsscsi</code>
     *
     * @throws StorageException
     */
    private void reinitEnclosureHostIds() throws StorageException
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
    }

    /**
     * Repopulates the local initiator ids that are also known by any Exos enclosure
     *
     * @throws AccessDeniedException
     * @throws StorageException
     */
    private void reinitInitiatorIds() throws StorageException, AccessDeniedException
    {
        // update initiator-ids...
        Set<String> localScsiInitiatorIds = SysClassUtils.getScsiInitiatorIds(extCmdFactory);

        Set<Long> maskedLocalScsiInitiatorIds = new HashSet<>();
        // see workaround-comment below
        for (String localScsiInitiatorIdStr : localScsiInitiatorIds)
        {
            maskedLocalScsiInitiatorIds.add(Long.parseLong(localScsiInitiatorIdStr, 16) & MASK_LSB_4);
        }

        HashSet<String> initiatorIds = new HashSet<>();

        // filter local scsi initiator ids for ids that Exos also knows
        for (ExosRestClient restClient : restClientMap.values())
        {
            if (restClient.hasAllRequiredPropsSet())
            {
                ExosRestInitiators exosInitiators = restClient.showInitiators();

                for (ExosRestInitiator exosInitiator : exosInitiators.initiator)
                {
                    /*
                     * Workaround:
                     * The local .../sas_address only includes the first (or one of) the Exos initiator ids.
                     * However, as Exos enumerates the initiator ids incrementally for all hosts, we simply mask the 4
                     * least
                     * significant bits when filtering
                     */
                    long maskedExosInitId = Long.parseLong(exosInitiator.id, 16) & MASK_LSB_4;
                    if (maskedLocalScsiInitiatorIds.contains(maskedExosInitId))
                    {
                        initiatorIds.add(exosInitiator.id);
                    }
                }
            }
        }

        errorReporter.logTrace("Using initiator ids: %s", initiatorIds);
        exosInitiatorIds = initiatorIds;
    }

    /**
     * Repopulates the <code>exosCtrlNameMapByTargetId</code> map, as well as sets the Exos port properties stating that
     * the specific port is connected to this node.
     *
     * @param localNodePropsRef
     *
     * @throws StorageException
     * @throws AccessDeniedException
     * @throws DatabaseException
     */
    private LocalPropsChangePojo reinitTargetIds(ReadOnlyProps localNodePropsRef)
        throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo ret = new LocalPropsChangePojo();

        // recache controller[].port[].target-id -> controller map
        Set<String> localScsiTargetIds = SysClassUtils.getScsiTargetIds(extCmdFactory);
        Map<String, String> exosCtrlNameMapByTargetIdNew = new HashMap<>();

        @Nullable ReadOnlyProps exosNamespace = localNodePropsRef.getNamespace(ApiConsts.NAMESPC_EXOS);
        if (exosNamespace != null)
        {
            for (String propKey : exosNamespace.map().keySet())
            {
                if (propKey.contains("/Ports/"))
                {
                    ret.deletedNodeProps.add(propKey);
                }
            }
        }

        try
        {
            for (ExosRestClient restClient : restClientMap.values())
            {
                if (restClient.hasAllRequiredPropsSet())
                {
                    ExosRestPorts exosRestPorts = restClient.showPorts();
                    for (ExosRestPort exosRestPort : exosRestPorts.port)
                    {
                        String enclosureName = restClient.getEnclosureName();
                        String propKey = ApiConsts.NAMESPC_EXOS + "/" +
                            enclosureName + "/" +
                            exosRestPort.controller +
                            "/Ports/" +
                            exosRestPort.port.substring(1); // "A0" -> "0"

                        if (localScsiTargetIds.contains(exosRestPort.targetId))
                        {
                            String currentValue = localNodePropsRef.getProp(propKey);
                            if (!Objects.equal(currentValue, ExosMappingManager.CONNECTED))
                            {
                                ret.changedNodeProps.put(propKey, ExosMappingManager.CONNECTED);
                            }

                            errorReporter.logDebug(
                                "Found connected port to enclosure: %s, port: %s",
                                enclosureName,
                                exosRestPort.port
                            );
                            exosCtrlNameMapByTargetIdNew.put(exosRestPort.targetId, exosRestPort.controller);
                        }
                        else
                        {
                            ret.deletedNodeProps.add(propKey);
                        }
                    }
                }
            }
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        exosCtrlNameMapByTargetId = exosCtrlNameMapByTargetIdNew;

        return ret;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPool)
        throws StorageException, AccessDeniedException
    {
        ReadOnlyProps props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getReadOnlyProps(storDriverAccCtx)
        );
        if (props.getProp(ApiConsts.KEY_STOR_POOL_NAME) == null)
        {
            throw new StorageException("Pool name must be set!");
        }

        return reinit();

        // String ids = localNodeProps.getProp(
        // ApiConsts.KEY_STOR_POOL_EXOS_INITIATOR_IDS,
        // ApiConsts.NAMESPC_STORAGE_DRIVER
        // );
        // if (ids == null)
        // {
        // throw new StorageException(
        // ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.KEY_STOR_POOL_EXOS_INITIATOR_IDS + " must be set"
        // );
        // }
        // PriorityProps prioProps = new PriorityProps(localNodeProps, stltConfigAccessor.getReadonlyProps());
        // ExceptionThrowingConsumer<String, StorageException> throwIfPropMissing = key ->
        // {
        // if (prioProps.getProp(key, ApiConsts.NAMESPC_STORAGE_DRIVER) == null)
        // {
        // throw new StorageException(key + " must be set");
        // }
        // };
        // throwIfPropMissing.accept(ApiConsts.KEY_STOR_POOL_EXOS_API_HOST);
        // throwIfPropMissing.accept(ApiConsts.KEY_STOR_POOL_EXOS_API_PASSWORD);
        // throwIfPropMissing.accept(ApiConsts.KEY_STOR_POOL_EXOS_API_USER);
    }


    private ExosRestClient getClient(StorPoolInfo storPoolRef) throws AccessDeniedException
    {
        return getClient(getEnclosureName(storPoolRef));
    }

    private ExosRestClient getClient(String enclosureName)
    {
        ExosRestClient restClient = restClientMap.get(enclosureName);
        if (restClient == null)
        {
            restClient = new ExosRestClient(storDriverAccCtx, errorReporter, stltConfigAccessor, enclosureName);
            restClientMap.put(enclosureName, restClient);
        }
        return restClient;
    }

    private String getEnclosureName(StorPoolInfo storPoolRef) throws InvalidKeyException, AccessDeniedException
    {
        return storPoolRef.getReadOnlyProps(storDriverAccCtx)
            .getProp(ApiConsts.KEY_STOR_POOL_EXOS_ENCLOSURE, ApiConsts.NAMESPC_EXOS);
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        return null;
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
        vlmData.setExpectedSize(size);
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

    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef)
    {
        return EXTENT_SIZE_IN_KIB;
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }
}
