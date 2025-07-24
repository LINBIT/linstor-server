package com.linbit.linstor.layer.storage.zfs;

import com.linbit.ImplementationError;
import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyStorPool;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.utils.PmemUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsCommands.ZfsVolumeType;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils;
import com.linbit.linstor.layer.storage.zfs.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.storage.utils.ZfsPropsUtils;
import com.linbit.utils.ShellUtils;
import com.linbit.utils.TimeUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

@Singleton
public class ZfsProvider extends AbsStorageProvider<ZfsInfo, ZfsData<Resource>, ZfsData<Snapshot>>
{
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    /** Format: "<code>{rscName}{rscNameSuffix}_{vlmNr}</code>" */
    public static final String FORMAT_RSC_TO_ZFS_ID = "%s%s_%05d";
    /**
     * <p>
     * Format: "<code>{FORMAT_RSC_TO_ZFS_ID}{rscFormatSuffix}@{snap_name}</code>"
     * </p>
     * <p>
     * <code>rscFormatSuffix</code> is only non-empty if the base resource should have been deleted but could not (i.e.
     * because it still has snapshot(s)). In this case the resource (including all of its snapshots) are renamed, given
     * a unique rscFormatSuffix
     * </p>
     */
    public static final String FORMAT_SNAP_TO_ZFS_ID = FORMAT_RSC_TO_ZFS_ID + "%s@%s";
    private static final String FORMAT_ZFS_DEV_PATH = "/dev/zvol/%s/%s";
    private static final int TOLERANCE_FACTOR = 3;

    private static final long ZFS_DFLT_EXTENT_SIZE_IN_KIB = 8L;
    private static final ExtToolsInfo.Version VERSION_2_0_0 = new ExtToolsInfo.Version(2, 0, 0);

    private static final String ZFS_DELETED_PREFIX = "_deleted_";

    /** This property will be stored in ZFS itself, NOT in a LINSTOR PropsContainer!
     *  Property key will be automatically prefixed with "linstor:" */
    private static final String ZFS_PROP_KEY_MARKED_FOR_DELETION = "marked_for_deletion";
    private static final String ZSF_PROP_VAL_TRUE = "true";

    private static final Map<String, BiConsumer<ZfsInfo, String>> DFLT_ZFS_LIST_WITH_PROPS_SETTER;

    static
    {
        DFLT_ZFS_LIST_WITH_PROPS_SETTER = new HashMap<>();
        DFLT_ZFS_LIST_WITH_PROPS_SETTER.put(
            ZFS_PROP_KEY_MARKED_FOR_DELETION,
            (zfsInfo, prop) -> zfsInfo.markedForDeletion = Boolean.parseBoolean(prop)
        );
    }

    private Map<StorPool, Long> extentSizes = new TreeMap<>();

    protected ZfsProvider(
        AbsStorageProviderInit superInitRef,
        String subTypeDescr,
        DeviceProviderKind kind
    )
    {
        super(superInitRef, subTypeDescr, kind);
    }

    @Inject
    public ZfsProvider(AbsStorageProviderInit superInitRef)
    {
        super(superInitRef, "ZFS", DeviceProviderKind.ZFS);
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.ZFS;
    }

    @Override
    public void clearCache() throws StorageException
    {
        super.clearCache();
        extentSizes.clear();
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        return ZfsUtils.getZPoolFreeSize(extCmdFactory.create(), changedStoragePoolStrings);
    }

    @Override
    protected Map<String, ZfsInfo> getInfoListImpl(
        List<ZfsData<Resource>> vlmDataListRef,
        List<ZfsData<Snapshot>> snapVlmsRef
    )
        throws StorageException
    {
        Set<String> dataSets = getDataSets(vlmDataListRef, snapVlmsRef);
        Set<String> poolDataSets = getZPoolDataSets(vlmDataListRef, snapVlmsRef);
        return ZfsUtils.getZfsList(
            extCmdFactory,
            dataSets,
            poolDataSets,
            kind,
            DFLT_ZFS_LIST_WITH_PROPS_SETTER
        );
    }

    private Set<String> getZPoolDataSets(List<ZfsData<Resource>> vlmDataListRef, List<ZfsData<Snapshot>> snapVlmsRef)
    {
        List<ZfsData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataListRef);
        combinedList.addAll(snapVlmsRef);

        Set<String> dataSets = new HashSet<>();
        for (ZfsData<?> data : combinedList)
        {
            dataSets.add(getZPool(data.getStorPool()));
        }

        return dataSets;
    }

    private Set<String> getDataSets(List<ZfsData<Resource>> vlmDataListRef, List<ZfsData<Snapshot>> snapVlmsRef)
    {

        Set<String> dataSets = new HashSet<>();
        for (ZfsData<Resource> data : vlmDataListRef)
        {
            dataSets.add(String.format("%s/%s", getZPool(data.getStorPool()), asLvIdentifier(data)));
        }
        for (ZfsData<Snapshot> data : snapVlmsRef)
        {
            final String snapIdentifier = asSnapLvIdentifierPrivileged(data);
            dataSets.add(String.format("%s/%s", getZPool(data.getStorPool()), getBaseLvIdentifier(snapIdentifier)));
            dataSets.add(String.format("%s/%s", getZPool(data.getStorPool()), snapIdentifier));
        }
        return dataSets;
    }

    @Override
    protected String asSnapLvIdentifier(ZfsData<Snapshot> snapVlmDataRef)
        throws AccessDeniedException
    {
        return asSnapLvIdentifier(snapVlmDataRef, false);
    }

    protected String asSnapLvIdentifierPrivileged(ZfsData<Snapshot> snapVlmDataRef)
    {
        try
        {
            return asSnapLvIdentifier(snapVlmDataRef, false);
        }
        catch (AccessDeniedException ignored)
        {
        }
        return "";
    }

    private String asSnapLvIdentifier(ZfsData<Snapshot> snapVlmDataRef, boolean forTakeSnapshotRef)
        throws AccessDeniedException
    {
        StorageRscData<Snapshot> snapData = snapVlmDataRef.getRscLayerObject();
        @Nullable String rscNameFormatSuffix;
        /**
         * If we need the snapLvId for creating a snapshot, the snapshot must not contain the rscNameFormatSuffix.
         * Otherwise the "snapshotExists" method might return false if a "_deleted_rsc_00000_1@snap" does not exist, but
         * "rsc_00000@snap" already exists.
         */
        if (!forTakeSnapshotRef)
        {
            rscNameFormatSuffix = snapData.getAbsResource()
                .getSnapProps(storDriverAccCtx)
                .getProp(InternalApiConsts.KEY_ZFS_RENAME_SUFFIX, STORAGE_NAMESPACE);
        }
        else
        {
            rscNameFormatSuffix = null;
        }
        return (rscNameFormatSuffix == null ? "" : ZFS_DELETED_PREFIX) + asSnapLvIdentifierRaw(
            snapData.getResourceName().displayValue,
            snapData.getResourceNameSuffix(),
            snapVlmDataRef.getVlmNr().value,
            rscNameFormatSuffix,
            snapData.getAbsResource().getSnapshotName().displayValue
        );
    }

    protected String asSnapLvIdentifierRaw(
        String rscNameRef,
        String rscNameSuffixRef,
        int vlmNrRef,
        @Nullable String rscFormatSuffixRef,
        String snapNameRef
    )
    {
        return String.format(
            FORMAT_SNAP_TO_ZFS_ID,
            rscNameRef,
            rscNameSuffixRef,
            vlmNrRef,
            rscFormatSuffixRef == null ? "" : ("_" + rscFormatSuffixRef),
            snapNameRef
        );
    }

    @Override
    protected String asLvIdentifier(
        @Nullable StorPoolName ignoredSpName,
        ResourceName resourceName,
        String rscNameSuffix,
        VolumeNumber volumeNumber
    )
    {
        return String.format(
            FORMAT_RSC_TO_ZFS_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @SuppressWarnings("unchecked")
    private String asIdentifierRaw(ZfsData<?> zfsData, ZfsVolumeType zfsTypeRef) throws AccessDeniedException
    {
        String identifier;
        switch (zfsTypeRef)
        {
            case VOLUME:
                identifier = asLvIdentifier((ZfsData<Resource>) zfsData);
                break;
            case SNAPSHOT:
                identifier = asSnapLvIdentifier((ZfsData<Snapshot>) zfsData);
                break;
            case FILESYSTEM: // fall-through
            default:
                throw new ImplementationError("Unexpected type: " + zfsTypeRef);
        }
        return identifier;
    }

    private String asFullQualifiedSnapIdentifier(ZfsData<Snapshot> zfsData, boolean forTakeSnapshotRef)
        throws AccessDeniedException
    {
        return getZPool(zfsData.getStorPool()) + File.separator +
            asSnapLvIdentifier(zfsData, forTakeSnapshotRef);
    }

    private String asFullQualifiedLvIdentifier(ReadOnlyVlmProviderInfo readOnlyVlmProvInfo)
    {
        return getZPool(readOnlyVlmProvInfo.getReadOnlyStorPool()) +
            File.separator +
            asLvIdentifier(
                null,
                readOnlyVlmProvInfo.getResourceName(),
                readOnlyVlmProvInfo.getRscSuffix(),
                readOnlyVlmProvInfo.getVlmNr()
            );
    }

    @Override
    protected void createLvImpl(ZfsData<Resource> vlmData)
        throws StorageException
    {
        ZfsCommands.create(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            vlmData.getExpectedSize(),
            false,
            getZfscreateOptions(vlmData)
        );
    }

    protected String[] getZfscreateOptions(ZfsData<Resource> vlmDataRef)
    {
        return getProp(vlmDataRef, ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_ZFS_CREATE_OPTIONS, "");
    }

    protected String[] getZfsSnapshotOptions(ZfsData<Resource> vlmDataRef)
    {
        return getProp(vlmDataRef, ApiConsts.NAMESPC_STORAGE_DRIVER, ApiConsts.KEY_STOR_POOL_ZFS_SNAPSHOT_OPTIONS, "");
    }

    protected String[] getProp(ZfsData<Resource> vlmDataRef, String namespace, String key, String dfltValue)
    {
        String[] additionalOptionsArr;

        try
        {
            String options = getPrioProps(vlmDataRef).getProp(
                key,
                namespace,
                dfltValue
            );
            List<String> additionalOptions = ShellUtils.shellSplit(options);
            additionalOptionsArr = new String[additionalOptions.size()];
            additionalOptions.toArray(additionalOptionsArr);
        }
        catch (AccessDeniedException | InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return additionalOptionsArr;
    }

    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef) throws StorageException
    {
        try
        {
            return ZfsPropsUtils.extractZfsVolBlockSizePrivileged(
                (ZfsData<?>) vlmDataRef,
                storDriverAccCtx,
                stltConfigAccessor.getReadonlyProps()
            );
        }
        catch (AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
    }

    /**
     * Brief summary of ZFS versions and default <code>volBlockSize</code>:
     * <ul>
     * <li>Before version 0.8.0, ZFS had no '<code>zfs version</code>' or '<code>zfs --version</code>'. Default
     * <code>volBlockSize</code> is 8k.</li>
     * <li>Before version 2.0.0, ZFS had no '<code>zfs create ... --dry-run --verbose --parseable</code>' options.
     * Default <code>volBlockSize</code> is still 8k.</li>
     * <li>Since ZFS version 2.2.0 the default <code>volBlockSize</code> is increased to 16k (openzfs git hash
     * <code>72f0521</code>).</li>
     * </ul>
     * This means, that LINSTOR can return <code>8k</code> for versions pre 2.0.0, and for versions since 2.0.0 "query"
     * the current <code>volBlockSize</code> using
     * '<code>zfs create zpool/dummy-volume -V 1B --dry-run --parseable</code>'. Here is an example result of this
     * command:
     *
     * <pre>
    # zfs create -n -P -V 1B scratch-zfs/dummy
    create  scratch-zfs/dummy
    property    volsize 8192
    property    refreservation  1843200
    #
     * </pre>
     */
    protected long getExtentSize(StorPool storPoolRef) throws StorageException
    {
        long ret = ZFS_DFLT_EXTENT_SIZE_IN_KIB;
        Map<ExtTools, ExtToolsInfo> externalTools = extToolsChecker.getExternalTools(false);
        @Nullable ExtToolsInfo zfsUtilsInfo = externalTools.get(ExtTools.ZFS_UTILS);
        @Nullable ExtToolsInfo zfsKmodInfo = externalTools.get(ExtTools.ZFS_KMOD);

        if (zfsUtilsInfo != null && zfsKmodInfo != null && zfsUtilsInfo.isSupported() && zfsKmodInfo.isSupported())
        {
            ExtToolsInfo.Version zfsUtilsVersion = zfsUtilsInfo.getVersion();
            ExtToolsInfo.Version zfsKmodVersion = zfsKmodInfo.getVersion();

            if (zfsUtilsVersion.getMajor() != null && zfsUtilsVersion.greaterOrEqual(VERSION_2_0_0) &&
                zfsKmodVersion.getMajor() != null && zfsKmodVersion.greaterOrEqual(VERSION_2_0_0))
            {
                ret = ZfsUtils.getZfsExtentSize(extCmdFactory.create(), getZPool(storPoolRef));
            }
        }
        return ret;
    }

    @Override
    protected void resizeLvImpl(ZfsData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.resize(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            vlmData.getExpectedSize()
        );
    }

    @Override
    protected void deleteLvImpl(ZfsData<Resource> vlmData, String ignored)
        throws StorageException, DatabaseException, AccessDeniedException
    {
        delete(vlmData, ZfsVolumeType.VOLUME);
    }

    @SuppressWarnings("unchecked")
    private void delete(ZfsData<?> vlmDataRef, ZfsVolumeType zfsTypeRef)
        throws StorageException, InvalidKeyException, AccessDeniedException, DatabaseException
    {
        ZfsInfo zfsInfo = infoListCache.get(vlmDataRef.getFullQualifiedLvIdentifier());
        if (canDelete(zfsInfo))
        {
            deleteCascading(vlmDataRef.getFullQualifiedLvIdentifier(), zfsTypeRef, true);
        }
        else
        {
            String newId;
            String oldId = vlmDataRef.getIdentifier();
            switch (zfsTypeRef)
            {
                case SNAPSHOT:
                    String[] split = oldId.split("@");
                    newId = split[0] + "@" + ZFS_DELETED_PREFIX + split[1] + "_" + TimeUtils.getZfsRenameTime();
                    break;
                case VOLUME:
                    newId = ZFS_DELETED_PREFIX + oldId +
                        getNextRenameSuffix((ZfsData<Resource>) vlmDataRef);
                    break;
                case FILESYSTEM: // fall-through
                default:
                    throw new ImplementationError("Unexpected type: " + zfsTypeRef);
            }
            markForDeletionAndRename(vlmDataRef, zfsTypeRef, newId);
        }
        vlmDataRef.setExists(false);
    }

    /**
     * Checks whether or not the given ZFS volume/snapshot can be deleted.
     * <p>A ZFS volume cannot be deleted if it still has ZFS snapshots</p>
     * <p>A ZFS snapshot cannot be deleted if it still has ZFS clones</p>
     *
     * @param zfsInfoRef
     * @param zfsTypeRef
     * @return
     */
    private boolean canDelete(ZfsInfo zfsInfoRef)
    {
        boolean ret;
        switch (zfsInfoRef.type)
        {
            case SNAPSHOT:
                ret = zfsInfoRef.clones.isEmpty();
                break;
            case VOLUME:
                ret = zfsInfoRef.snapshots.isEmpty();
                break;
            case FILESYSTEM: // fall-through
            default:
                throw new ImplementationError("Unexpected type: " + zfsInfoRef.type);
        }
        return ret;
    }

    private void deleteCascading(String fullQualIdentifierRef, ZfsVolumeType zfsTypeRef, boolean deleteUnconditionalRef)
        throws StorageException
    {
        @Nullable ZfsInfo zfsInfo = infoListCache.get(fullQualIdentifierRef);
        if (zfsInfo == null)
        {
            errorReporter.logWarning(
                "Should delete ZFS %s %s, but it was not found. noop",
                zfsTypeRef.getDescr(),
                fullQualIdentifierRef
            );
        }
        else
        {
            final boolean delete = deleteUnconditionalRef || zfsInfo.markedForDeletion;
            if (delete)
            {
                ZfsCommands.delete(
                    extCmdFactory.create(),
                    fullQualIdentifierRef,
                    zfsTypeRef
                );

                deleteCascade(zfsInfo, zfsTypeRef);
            }
        }
    }

    private void deleteCascade(ZfsInfo zfsInfoRef, ZfsVolumeType zfsTypeRef) throws StorageException
    {
        final @Nullable String originOrBaseVolume;
        final ZfsVolumeType originOrBaseType;
        if (zfsTypeRef.equals(ZfsVolumeType.VOLUME))
        {
            originOrBaseVolume = zfsInfoRef.originStr;
            originOrBaseType = ZfsVolumeType.SNAPSHOT;
        }
        else
        {
            originOrBaseVolume = zfsInfoRef.poolName + File.separator + getBaseLvIdentifier(zfsInfoRef.identifier);
            originOrBaseType = ZfsVolumeType.VOLUME;
        }
        if (originOrBaseVolume != null)
        {
            // tell our parent ZFS zvol/snapshot that we got deleted, so that cascading can properly work
            @Nullable ZfsInfo originZfsInfo = loadZfsInfo(originOrBaseVolume);
            if (originZfsInfo == null)
            {
                StringBuilder sb = new StringBuilder("Failed to find ")
                    .append(zfsTypeRef.getDescr())
                    .append(" ")
                    .append(zfsInfoRef.poolName)
                    .append(File.separator)
                    .append(zfsInfoRef.identifier)
                    .append("'s ");
                if (zfsTypeRef == ZfsVolumeType.SNAPSHOT)
                {
                    sb.append("base volume '");
                }
                else
                {
                    sb.append("origin snapshot '");
                }
                sb.append(originOrBaseVolume)
                    .append("'. Cascading deletion failed. Manual cleanup might be necessary");
                throw new StorageException(sb.toString());
            }

            if (zfsTypeRef.equals(ZfsVolumeType.SNAPSHOT))
            {
                originZfsInfo.snapshots.remove(zfsInfoRef); // just in case we loaded this from cache
            }
            else
            {
                originZfsInfo.clones.remove(zfsInfoRef);
            }

            if (originZfsInfo.markedForDeletion && canDelete(originZfsInfo))
            {
                deleteCascading(originOrBaseVolume, originOrBaseType, false);
            }
        }
    }

    private @Nullable ZfsInfo loadZfsInfo(String fullQualifiedIdentifierRef) throws StorageException
    {
        @Nullable ZfsInfo ret = infoListCache.get(fullQualifiedIdentifierRef);
        if (ret == null)
        {
            HashMap<String, ZfsInfo> zfsList = ZfsUtils.getZfsList(
                extCmdFactory,
                Collections.singleton(fullQualifiedIdentifierRef),
                Collections.singleton(fullQualifiedIdentifierRef),
                kind,
                DFLT_ZFS_LIST_WITH_PROPS_SETTER
            );
            ret = zfsList.get(fullQualifiedIdentifierRef);
            if (ret != null)
            {
                infoListCache.put(fullQualifiedIdentifierRef, ret);
            }
        }
        return ret;
    }

    private String getNextRenameSuffix(ZfsData<Resource> vlmDataRef) throws InvalidKeyException, AccessDeniedException
    {
        String nextNum = vlmDataRef.getRscLayerObject()
            .getAbsResource()
            .getProps(storDriverAccCtx)
            .getProp(InternalApiConsts.KEY_ZFS_RENAME_SUFFIX, STORAGE_NAMESPACE);
        return "_" + nextNum;
    }

    @Override
    protected void deactivateLvImpl(ZfsData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // noop, not supported
    }

    @Override
    public boolean snapshotExists(ZfsData<Snapshot> snapVlm, boolean forTakeSnapshotRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsInfo zfsInfo = infoListCache.get(asFullQualifiedSnapIdentifier(snapVlm, forTakeSnapshotRef));
        return zfsInfo != null;
    }

    @Override
    protected void createSnapshot(ZfsData<Resource> vlmData, ZfsData<Snapshot> snapVlm, boolean readOnly)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Snapshot snap = snapVlm.getVolume().getAbsResource();
        // snapshot will be created by "zfs receive" command
        if (!snap.getFlags().isSet(storDriverAccCtx, Snapshot.Flags.SHIPPING_TARGET))
        {
            ZfsCommands.createSnapshot(
                extCmdFactory.create(),
                vlmData.getZPool(),
                asLvIdentifier(vlmData),
                snapVlm.getVolume().getAbsResource().getSnapshotName().displayValue,
                getZfsSnapshotOptions(vlmData)
            );
        }
    }

    @Override
    protected void restoreSnapshot(ZfsData<Snapshot> sourceSnapVlmDataRef, ZfsData<Resource> targetVlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.restoreSnapshot(
            extCmdFactory.create(),
            targetVlmDataRef.getZPool(),
            asSnapLvIdentifier(sourceSnapVlmDataRef),
            asLvIdentifier(targetVlmDataRef)
        );
    }

    @Override
    protected void deleteSnapshotImpl(ZfsData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        delete(snapVlmRef, ZfsVolumeType.SNAPSHOT);
    }

    private void markForDeletionAndRename(ZfsData<?> zfsDataRef, ZfsVolumeType zfsVlmTypeRef, String newId)
        throws StorageException, AccessDeniedException
    {
        final String zPool = getZPool(zfsDataRef.getStorPool());
        final String rawCurrentLvId = asIdentifierRaw(zfsDataRef, zfsVlmTypeRef);
        markForDeletion(zPool, rawCurrentLvId);
        ZfsCommands.rename(
            extCmdFactory.create(),
            zPool,
            rawCurrentLvId,
            newId
        );
    }

    private void markForDeletion(final String zPoolRef, final String zfsIdRef) throws StorageException
    {
        ZfsCommands.setUserProperty(
            extCmdFactory.create(),
            zPoolRef,
            zfsIdRef,
            ZFS_PROP_KEY_MARKED_FOR_DELETION,
            ZSF_PROP_VAL_TRUE
        );
    }

    @Override
    protected void rollbackImpl(ZfsData<Resource> vlmDataRef, ZfsData<Snapshot> rollbackToSnapVlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.rollback(
            extCmdFactory.create(),
            rollbackToSnapVlmDataRef.getZPool(),
            asLvIdentifier(vlmDataRef),
            rollbackToSnapVlmDataRef.getRscLayerObject().getSnapName().displayValue
        );
    }

    @Override
    public String getDevicePath(String zPool, String identifier)
    {
        return String.format(FORMAT_ZFS_DEV_PATH, zPool, identifier);
    }

    @Override
    protected String getStorageName(StorPool storPoolRef)
    {
        return getZPool(storPoolRef);
    }

    protected @Nullable String getZPool(StorPoolInfo storPool)
    {
        String zPool;
        try
        {
            zPool = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getReadOnlyProps(storDriverAccCtx)
            ).getProp(StorageConstants.CONFIG_ZFS_POOL_KEY);
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return zPool;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPool)
        throws StorageException, AccessDeniedException
    {
        String zpoolName = getZPool(storPool);
        if (zpoolName == null)
        {
            throw new StorageException(
                "zPool name not given for storPool '" +
                    storPool.getName().displayValue + "'"
            );
        }
        zpoolName = zpoolName.trim();
        HashMap<String, ZfsInfo> zfsPoolMap = ZfsUtils.getThinZPoolsList(
            extCmdFactory.create(),
            Collections.singleton(zpoolName)
        );
        if (!zfsPoolMap.containsKey(zpoolName))
        {
            throw new StorageException("no zpool found with name '" + zpoolName + "'");
        }

        return null;
    }

    protected void checkExtentSize(StorPool storPool, LocalPropsChangePojo ret)
        throws ImplementationError, AccessDeniedException, StorageException
    {
        @Nullable String oldExtentSizeInKib = storPool.getProps(storDriverAccCtx)
            .getProp(
                InternalApiConsts.ALLOCATION_GRANULARITY,
                StorageConstants.NAMESPACE_INTERNAL
            );

        long currentExtentSize = getExtentSize(storPool);
        String currentExtentSizeStr = Long.toString(currentExtentSize);

        if (!Objects.equals(currentExtentSizeStr, oldExtentSizeInKib))
        {
            errorReporter.logTrace(
                "Found extent size for ZFS SP '%s': %dKiB",
                storPool.getName().displayValue,
                currentExtentSize
            );
            ret.changeStorPoolProp(
                storPool,
                StorageConstants.NAMESPACE_INTERNAL + "/" + InternalApiConsts.ALLOCATION_GRANULARITY,
                currentExtentSizeStr
            );
        }
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        LocalPropsChangePojo ret = new LocalPropsChangePojo();
        List<String> pvs = ZfsUtils.getPhysicalVolumes(extCmdFactory.create(), getZpoolOnlyName(storPoolRef));
        if (PmemUtils.supportsDax(extCmdFactory.create(), pvs))
        {
            storPoolRef.setPmem(true);
        }
        checkExtentSize(storPoolRef, ret);

        return ret;
    }

    protected String getZpoolOnlyName(StorPool storPoolRef) throws StorageException
    {
        String zpoolName = getZPool(storPoolRef);
        if (zpoolName == null)
        {
            throw new StorageException("Unset zfs dataset for " + storPoolRef);
        }

        return ZfsUtils.getZPoolRootName(zpoolName);
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException, AccessDeniedException
    {
        String zPool = getZPool(storPool);
        if (zPool == null)
        {
            throw new StorageException("Unset zfs dataset for " + storPool);
        }

        long capacity = ZfsUtils.getZPoolTotalSize(
            extCmdFactory.create(),
            Collections.singleton(zPool)
        ).get(zPool);
        long freeSpace = ZfsUtils.getZPoolFreeSize(
            extCmdFactory.create(),
            Collections.singleton(zPool)
        ).get(zPool);

        return SpaceInfo.buildOrThrowOnError(capacity, freeSpace, storPool);
    }

    @Override
    protected boolean updateDmStats()
    {
        return false;
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return false;
    }

    // @Override
    // protected void startReceiving(ZfsData<Resource> vlmDataRef, ZfsData<Snapshot> snapVlmDataRef)
    // throws AccessDeniedException, StorageException, DatabaseException
    // {
    // try
    // {
    // /*
    // * if this is the initial shipment, the actual volume must not exist.
    // */
    // Snapshot targetSnap = snapVlmDataRef.getRscLayerObject().getAbsResource();
    // SnapshotDefinition snapDfn = targetSnap.getSnapshotDefinition();
    // ResourceDefinition rscDfn = snapDfn.getResourceDefinition();
    //
    // String sourceNodeName = snapDfn.getProps(storDriverAccCtx)
    // .getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_SOURCE_NODE);
    // Resource targetRsc = rscDfn.getResource(storDriverAccCtx, targetSnap.getNodeName());
    // Resource sourceRsc = rscDfn.getResource(storDriverAccCtx, new NodeName(sourceNodeName));
    //
    // String prevShippingSnapName = targetRsc.getAbsResourceConnection(storDriverAccCtx, sourceRsc)
    // .getProps(storDriverAccCtx)
    // .getProp(InternalApiConsts.KEY_SNAPSHOT_SHIPPING_NAME_PREV);
    //
    // if (prevShippingSnapName == null)
    // {
    // deleteLvImpl(vlmDataRef, asLvIdentifier(vlmDataRef));
    // vlmDataRef.setInitialShipment(true);
    // }
    // }
    // catch (InvalidNameException exc)
    // {
    // throw new ImplementationError(exc);
    // }
    //
    // super.startReceiving(vlmDataRef, snapVlmDataRef);
    // }

    @Override
    protected String getSnapshotShippingReceivingCommandImpl(ZfsData<Snapshot> snapVlmDataRef)
        throws StorageException, AccessDeniedException
    {
        return "zfs receive -F " + getZPool(snapVlmDataRef.getStorPool()) + "/" +
            asSnapLvIdentifier(snapVlmDataRef);
    }

    @Override
    protected String getSnapshotShippingSendingCommandImpl(
        ZfsData<Snapshot> lastSnapVlmDataRef,
        ZfsData<Snapshot> curSnapVlmDataRef
    )
        throws StorageException, AccessDeniedException
    {
        StringBuilder sb = new StringBuilder("zfs send ");
        if (lastSnapVlmDataRef != null)
        {
            sb.append("-i ") // incremental
                .append(getZPool(lastSnapVlmDataRef.getStorPool())).append("/")
                .append(asSnapLvIdentifier(lastSnapVlmDataRef)).append(" ");
        }
        sb.append(getZPool(curSnapVlmDataRef.getStorPool())).append("/")
            .append(asSnapLvIdentifier(curSnapVlmDataRef));
        return sb.toString();
    }

    @Override
    protected void finishShipReceiving(ZfsData<Resource> vlmDataRef, ZfsData<Snapshot> snapVlmRef)
        throws StorageException, DatabaseException, AccessDeniedException
    {
        vlmDataRef.setInitialShipment(false);
        // String vlmDataId = asLvIdentifier(vlmDataRef);
        // deleteLvImpl(vlmDataRef, vlmDataId); // delete calls "vlmData.setExists(false);" - we have to undo this
        // ZfsCommands.rename(
        // extCmdFactory.create(),
        // vlmDataRef.getZPool(),
        // asSnapLvIdentifier(snapVlmRef),
        // vlmDataId
        // );
        // vlmDataRef.setExists(true);
    }

    @Override
    protected void updateStates(List<ZfsData<Resource>> vlmDataList, List<ZfsData<Snapshot>> snapVlms)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        /*
         *  updating volume states
         */
        for (ZfsData<Resource> vlmData : vlmDataList)
        {
            updateState(vlmData, ZfsVolumeType.VOLUME);
        }
        for (ZfsData<Snapshot> snapVlmData : snapVlms)
        {
            updateState(snapVlmData, ZfsVolumeType.SNAPSHOT);
        }
    }

    private void updateState(ZfsData<?> vlmData, ZfsVolumeType zfsTypeRef)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        vlmData.setZPool(getZPool(vlmData.getStorPool()));

        vlmData.setIdentifier(asIdentifierRaw(vlmData, zfsTypeRef));
        String fullQualifiedLvIdentifier = vlmData.getFullQualifiedLvIdentifier();
        @Nullable ZfsInfo info = infoListCache.get(fullQualifiedLvIdentifier);

        vlmData.setMarkedForDeletion(false);
        if (info != null)
        {
            updateInfo(vlmData, info);

            final long expectedSize = vlmData.getExpectedSize();
            final long actualSize = info.usableSize;
            if (actualSize != expectedSize && zfsTypeRef.equals(ZfsVolumeType.VOLUME))
            {
                if (actualSize < expectedSize)
                {
                    vlmData.setSizeState(Size.TOO_SMALL);
                }
                else
                {
                    long extentSize = ZfsUtils.getZfsExtentSize(
                        extCmdFactory.create(),
                        info.poolName,
                        info.identifier
                    );
                    vlmData.setSizeState(Size.TOO_LARGE);
                    final long toleratedSize = expectedSize + extentSize * TOLERANCE_FACTOR;
                    if (actualSize < toleratedSize)
                    {
                        vlmData.setSizeState(Size.TOO_LARGE_WITHIN_TOLERANCE);
                    }
                }
            }
            else
            {
                vlmData.setSizeState(Size.AS_EXPECTED);
            }
        }
        else
        {
            vlmData.setExists(false);
            vlmData.setDevicePath(null);
            vlmData.setAllocatedSize(-1);
        }
    }

    private String getBaseLvIdentifier(String identifier)
    {
        return identifier.substring(0, identifier.indexOf("@"));
    }

    @SuppressWarnings("unchecked")
    private void updateInfo(ZfsData<?> vlmData, ZfsInfo zfsInfo) throws DatabaseException, AccessDeniedException
    {
        vlmData.setExists(true);
        vlmData.setZPool(zfsInfo.poolName);
        vlmData.setIdentifier(zfsInfo.identifier);
        vlmData.setAllocatedSize(zfsInfo.allocatedSize);
        vlmData.setUsableSize(zfsInfo.usableSize);
        vlmData.setExtentSize(zfsInfo.volBlockSize);

        String devicePath;

        if (vlmData.getRscLayerObject().getAbsResource() instanceof Resource &&
            isCloning((ZfsData<Resource>) vlmData))
        {
            /*
             * while the volume is cloning, we do not want to set the device path so that other tools do not try to
             * access it
             */
            devicePath = null;
        }
        else
        {
            devicePath = zfsInfo.path;
        }

        vlmData.setDevicePath(devicePath);
    }

    protected PriorityProps getPrioProps(ZfsData<Resource> vlmDataRef) throws AccessDeniedException
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
            rsc.getNode().getProps(storDriverAccCtx),
            vlmDfn.getProps(storDriverAccCtx),
            rscDfn.getProps(storDriverAccCtx),
            rscGrp.getVolumeGroupProps(storDriverAccCtx, vlmDfn.getVolumeNumber()),
            rscGrp.getProps(storDriverAccCtx),
            stltConfigAccessor.getReadonlyProps()
        );
    }
    @Override
    protected void setDevicePath(ZfsData<Resource> vlmDataRef, String devicePathRef) throws DatabaseException
    {
        vlmDataRef.setDevicePath(devicePathRef);
    }

    @Override
    protected void setAllocatedSize(ZfsData<Resource> vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setAllocatedSize(sizeRef);
    }

    @Override
    protected void setUsableSize(ZfsData<Resource> vlmDataRef, long sizeRef) throws DatabaseException
    {
        vlmDataRef.setUsableSize(sizeRef);
    }

    @Override
    protected void setExpectedUsableSize(ZfsData<Resource> vlmData, long size)
        throws DatabaseException, StorageException
    {
        vlmData.setExpectedSize(size);
    }

    @Override
    protected String getStorageName(ZfsData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getZPool();
    }

    @Override
    protected void createSnapshotForCloneImpl(
        ZfsData<Resource> vlmData,
        String cloneRscName)
        throws StorageException
    {
        final String srcFullSnapshotName = getCloneSnapshotNameFull(vlmData, cloneRscName, "@");


        if (!infoListCache.containsKey(vlmData.getZPool() + "/" + srcFullSnapshotName))
        {
            ZfsCommands.createSnapshotFullName(
                extCmdFactory.create(),
                vlmData.getZPool(),
                srcFullSnapshotName
            );
            // mark snapshot as temporary clone
            ZfsCommands.setUserProperty(
                extCmdFactory.create(),
                vlmData.getZPool(),
                srcFullSnapshotName,
                "clone_for",
                cloneRscName
            );

            // mark to be deleted
            markForDeletion(vlmData.getZPool(), srcFullSnapshotName);
        }
        else
        {
            errorReporter.logInfo("Clone base snapshot %s already found, reusing.", srcFullSnapshotName);
        }
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException, AccessDeniedException
    {
        Map<ReadOnlyVlmProviderInfo, Long> ret = new HashMap<>();
        Set<String> dataSets = new HashSet<>();
        Set<String> poolDataSets = new HashSet<>();

        for (ReadOnlyVlmProviderInfo roVlmProvInfo : vlmDataListRef)
        {
            ReadOnlyStorPool roStorPool = roVlmProvInfo.getReadOnlyStorPool();
            @Nullable String zpool = getZPool(roStorPool);
            poolDataSets.add(zpool);
            dataSets.add(String.format("%s/%s", zpool, roVlmProvInfo.getIdentifier()));
        }

        HashMap<String, ZfsInfo> zfsListMap = ZfsUtils.getZfsList(
            extCmdFactory,
            dataSets,
            poolDataSets,
            kind,
            Collections.emptyMap()
        );

        for (ReadOnlyVlmProviderInfo roVlmProvInfo : vlmDataListRef)
        {
            @Nullable String identifier = asFullQualifiedLvIdentifier(roVlmProvInfo);
            @Nullable ZfsInfo zfsInfo = zfsListMap.get(identifier);
            long allocatedSize;
            if (zfsInfo == null)
            {
                allocatedSize = roVlmProvInfo.getOrigAllocatedSize();
            }
            else
            {
                // zfsInfo.size should already be in KiB
                allocatedSize = zfsInfo.allocatedSize;
            }
            ret.put(roVlmProvInfo, allocatedSize);
        }

        return ret;
    }

    @Override
    public void openForClone(VlmProviderObject<?> vlm, @Nullable String cloneName, boolean readOnly)
        throws StorageException
    {
        ZfsData<Resource> srcData = (ZfsData<Resource>) vlm;
        String zPool = getZPool(srcData.getStorPool());
        if (cloneName != null)
        {
            final String fullSnapshotName = getCloneSnapshotNameFull(srcData, cloneName, "@");
            ZfsCommands.hideUnhideSnapshotDevice(
                extCmdFactory.create(), zPool, srcData.getIdentifier(), false);
            vlm.setCloneDevicePath(getDevicePath(zPool, fullSnapshotName));
        }
        else
        {
            createLvImpl(srcData);
            final String devicePath = getDevicePath(zPool, asLvIdentifier(srcData));
            try
            {
                waitUntilDeviceCreated(srcData, devicePath);
            }
            catch (AccessDeniedException exc)
            {
                throw new StorageException("Unable to run openForClone::waitUntilDeviceCreated", exc);
            }
            vlm.setCloneDevicePath(devicePath);
        }
    }

    @Override
    public void closeForClone(VlmProviderObject<?> vlm, @Nullable String cloneName) throws StorageException
    {
        ZfsData<Resource> srcData = (ZfsData<Resource>) vlm;
        // This hide doesn't make problems for other concurernt clones of the same base resource
        // It was tested and worked without any problems.
        ZfsCommands.hideUnhideSnapshotDevice(
            extCmdFactory.create(), getZPool(srcData.getStorPool()), srcData.getIdentifier(), true);
        vlm.setCloneDevicePath(null);
    }
}
