package com.linbit.linstor.layer.storage.lvm;

import com.linbit.ImplementationError;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.SnapshotDefinition;
import com.linbit.linstor.core.objects.SnapshotVolume;
import com.linbit.linstor.core.objects.SnapshotVolumeDefinition;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.StorageLayerSizeCalculator;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LvmLockMode;
import com.linbit.linstor.layer.storage.lvm.utils.LvmCommands.LvmVolumeType;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.layer.storage.lvm.utils.LvmUtils.VgsInfo;
import com.linbit.linstor.layer.storage.utils.PmemUtils;
import com.linbit.linstor.layer.storage.utils.SharedStorageUtils;
import com.linbit.linstor.layer.storage.utils.StorageConfigReader;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.InvalidValueException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.lvm.LvmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.utils.ShellUtils;
import com.linbit.utils.StringUtils;
import com.linbit.utils.TimeUtils;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Singleton
public class LvmProvider
    extends AbsStorageProvider<LvsInfo, LvmData<Resource>, LvmData<Snapshot>>
{
    private static final int TOLERANCE_FACTOR = 3;
    // FIXME: FORMAT should be private, only made public for LayeredSnapshotHelper
    public static final String FORMAT_RSC_TO_LVM_ID = "%s%s_%05d";
    public static final String FORMAT_SNAP_TO_LVM_ID = FORMAT_RSC_TO_LVM_ID + "_%s";

    private static final String DFLT_LVCREATE_TYPE = "linear";

    private static final AtomicLong DELETED_ID = new AtomicLong(0);

    /** @see com.linbit.linstor.layer.storage.zfs.ZfsProvider - uses the same prefix for renamed origins */
    private static final String LVM_DELETED_PREFIX = "_deleted_";
    private static final String FORMAT_LVM_DELETED_ID = LVM_DELETED_PREFIX + "%s_%s";
    /** Matches the {@link TimeUtils#getRenameTime()} part of a {@link #FORMAT_LVM_DELETED_ID} name */
    private static final Pattern LVM_DELETED_TIME_PATTERN = Pattern.compile(
        "\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}-\\d{3}"
    );

    private static final String DFLT_RESTORE_DD_BLOCKSIZE = "64k";

    /**
     * The lvmlockd lock mode each LV was last activated with, keyed "&lt;vg&gt;/&lt;lv&gt;". Only
     * maintained for volumes in externally locked storage pools (non-default lock modes): converting
     * an already held lock is a noop for lvmlockd, but costs an external lvchange call per volume per
     * dispatch, so the conversion is skipped while the LV is active and the required mode was applied
     * before. Entries are dropped when the LV is deactivated (which also releases its lock), deleted
     * or renamed.
     */
    private final Map<String, LvmLockMode> appliedLockModes = new HashMap<>();

    protected LvmProvider(
        AbsStorageProviderInit superInitRef,
        String subTypeDescr,
        DeviceProviderKind subTypeKind
    )
    {
        super(superInitRef, subTypeDescr, subTypeKind);
    }

    @Inject
    public LvmProvider(AbsStorageProviderInit superInitRef)
    {
        super(superInitRef, "LVM", DeviceProviderKind.LVM);
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.LVM;
    }

    @Override
    protected void updateStates(List<LvmData<Resource>> vlmDataList, List<LvmData<Snapshot>> snapVlmDataList)
        throws StorageException, DatabaseException
    {
        final Map<String, VgsInfo> extentSizes = LvmUtils.getVgsInfo(
            extCmdFactory,
            getAffectedVolumeGroups(vlmDataList, snapVlmDataList),
            false
        );

        List<LvmData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlmDataList);

        for (LvmData<?> vlmData : combinedList)
        {
            final LvsInfo info = infoListCache.get(getFullQualifiedIdentifier(vlmData));
            updateInfo(vlmData, info);

            // final VlmStorageState<T> vlmState = vlmStorStateFactory.create((T) info, vlm);

            if (info != null)
            {
                final long expectedSize = vlmData.getExpectedSize();
                final long actualSize = getUsableSize(info);
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
                            expectedSize + extentSizes.get(info.volumeGroup).vgExtentSize * TOLERANCE_FACTOR;
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

    /**
     * For thick snapshots "lvs" reports the size of the CoW area, which is the allocated size. The usable
     * (virtual) size of the snapshot device equals its origin's size, so look that up instead - this also
     * works for renamed ("<code>_deleted_</code>") origins. For everything else (volumes, thin snapshots)
     * the reported size is both allocated and usable size.
     */
    protected long getUsableSize(LvsInfo infoRef)
    {
        long usableSize = infoRef.size;
        if (infoRef.origin != null && infoRef.thinPool == null)
        {
            @Nullable LvsInfo originInfo = infoListCache.get(
                infoRef.volumeGroup + File.separator + infoRef.origin
            );
            if (originInfo != null)
            {
                usableSize = originInfo.size;
            }
        }
        return usableSize;
    }

    protected String getFullQualifiedIdentifier(LvmData<?> vlmDataRef)
    {
        return vlmDataRef.getVolumeGroup() +
            File.separator +
            asIdentifierRaw(vlmDataRef);
    }

    @SuppressWarnings("unchecked")
    protected String asIdentifierRaw(LvmData<?> vlmData)
    {
        String identifier;
        if (vlmData.getVolume() instanceof Volume)
        {
            identifier = asLvIdentifier((LvmData<Resource>) vlmData);
        }
        else
        {
            identifier = asSnapLvIdentifier((LvmData<Snapshot>) vlmData);
        }
        return identifier;
    }

    /*
     * Expected to be overridden (extended) by LvmThinProvider
     */
    @SuppressWarnings({ "unchecked" })
    protected void updateInfo(LvmData<?> vlmDataRef, @Nullable LvsInfo info)
        throws DatabaseException, StorageException
    {
        boolean setDevicePath;
        String lvcreateOptions;
        LvmLockMode lockMode = LvmLockMode.DEFAULT;
        @Nullable LvmData<Resource> rscVlmData = null;
        if (vlmDataRef.getVolume() instanceof Volume)
        {
            LvmData<Resource> vlmData = (LvmData<Resource>) vlmDataRef;
            rscVlmData = vlmData;
            vlmDataRef.setIdentifier(asLvIdentifier(vlmData));
            setDevicePath = !vlmData.getVolume().getAbsResource().getStateFlags()
                .isSet(Resource.Flags.INACTIVE);
            /*
             * while the volume is cloning, we do not want to set the device path so that other tools do not try to
             * access it
             */
            setDevicePath &= !isCloning(vlmData);

            lockMode = getRequiredLockMode(vlmData, info);
            lvcreateOptions = getLvcreateOptions(vlmData);
        }
        else
        {
            LvmData<Snapshot> snapVlmData = (LvmData<Snapshot>) vlmDataRef;
            vlmDataRef.setIdentifier(asSnapLvIdentifier(snapVlmData));
            /*
             * A snapshot flagged for deletion must not be (re-)activated: activating a thick snapshot
             * implicitly also activates its origin, which "lvremove <snapshot>" would leave active - fatal
             * for INACTIVE resources in shared storage pools. lvremove works on inactive LVs anyways.
             */
            Snapshot snap = snapVlmData.getRscLayerObject().getAbsResource();
            setDevicePath = !snap.getFlags().isSet(Snapshot.Flags.DELETE) &&
                !isSnapshotLvBarredFromActivation(snapVlmData);

            lvcreateOptions = getLvcreateSnapshotOptions(vlmDataRef);
        }

        if (info == null)
        {
            vlmDataRef.setExists(false);
            vlmDataRef.setVolumeGroup(extractVolumeGroup(vlmDataRef));
            vlmDataRef.setDevicePath(null);
            vlmDataRef.setAllocatedSize(-1);
            // vlmData.setUsableSize(-1);
            vlmDataRef.setAttributes(null);
            if (rscVlmData != null)
            {
                rscVlmData.setActive(false);
            }

            List<String> additionalOptions = ShellUtils.shellSplit(lvcreateOptions);
            String[] additionalOptionsArr = new String[additionalOptions.size()];
            additionalOptions.toArray(additionalOptionsArr);
            updateStripesPropIfNeeded(vlmDataRef, findStripesInAdditionalArgs(additionalOptionsArr));
        }
        else
        {
            vlmDataRef.setExists(true);
            vlmDataRef.setVolumeGroup(info.volumeGroup);
            if (setDevicePath)
            {
                vlmDataRef.setDevicePath(info.path);
            }
            else
            {
                vlmDataRef.setDevicePath(null);
            }
            vlmDataRef.setIdentifier(info.identifier);
            vlmDataRef.setAllocatedSize(info.size);
            vlmDataRef.setUsableSize(getUsableSize(info));
            vlmDataRef.setAttributes(info.attributes);

            boolean lvActive = info.attributes.contains("a");
            /*
             * With a non-default lock mode the activation command is also run for an already active
             * LV: it converts the persistent lvmlockd LV lock if it is held in the other mode and is
             * a noop otherwise. The noop case is skipped based on the last applied mode, saving an
             * external lvchange call per volume on every dispatch.
             */
            String lockKey = lockModeKey(vlmDataRef.getVolumeGroup(), vlmDataRef.getIdentifier());
            boolean convertLock = lockMode != LvmLockMode.DEFAULT && lockMode != appliedLockModes.get(lockKey);
            if (setDevicePath && (!lvActive || convertLock))
            {
                final LvmLockMode lockModeFinal = lockMode;
                LvmUtils.execWithRetry(
                    extCmdFactory,
                    Collections.singleton(vlmDataRef.getVolumeGroup()),
                    config -> LvmCommands.activateVolume(
                        extCmdFactory.create(),
                        vlmDataRef.getVolumeGroup(),
                        vlmDataRef.getIdentifier(),
                        config,
                        lockModeFinal
                    )
                );
                if (lockMode != LvmLockMode.DEFAULT)
                {
                    appliedLockModes.put(lockKey, lockMode);
                }
                if (!lvActive)
                {
                    LvmUtils.recacheNextLvs();
                }
            }
            // deactivating a volume MUST NOT happen within the prepare step
            // as other layers might still hold the device open

            if (rscVlmData != null)
            {
                /*
                 * After the block above the LV is active whenever setDevicePath is set, even if "lvs"
                 * reported it inactive. Still active renamed origins count as active so the volume
                 * takes the deactivation path, releasing their device nodes and lvmlockd locks.
                 */
                rscVlmData.setActive(
                    setDevicePath || lvActive || !getActiveRenamedOrigins(rscVlmData).isEmpty()
                );
            }

            updateStripesPropIfNeeded(vlmDataRef, info.stripes);
        }
    }

    /**
     * The lvmlockd lock mode the LV of the given volume has to be activated with. In storage pools
     * with external locking the LV lock is held exclusively as long as this resource is the only one
     * using the shared LV. As soon as another resource (leg) of the rsc-dfn shares the LV - e.g. the
     * target of a live migration - the lock is downgraded to a shared lock so both legs can be active
     * at once, and upgraded back to an exclusive lock after the second leg is removed.
     *
     * Snapshots take precedence over sharing: lvmlockd requires the origin LV's lock in exclusive
     * mode both to create a snapshot and for as long as any snapshot of the LV exists. The lock is
     * therefore upgraded before a (clone-)snapshot is created and only downgraded back to a shared
     * lock once the LV has no snapshot LVs left.
     *
     * The same applies to resizing: lvresize needs the LV lock in exclusive mode, so a pending resize
     * upgrades the lock for the duration of the resize (the controller refuses resizes while the LV
     * is active on more than one node, so no other leg holds the lock at that point).
     */
    private LvmLockMode getRequiredLockMode(LvmData<Resource> vlmDataRef, @Nullable LvsInfo infoRef)
    {
        LvmLockMode lockMode = LvmLockMode.DEFAULT;
        StorPool storPool = vlmDataRef.getStorPool();
        if (storPool.isExternalLocking() && storPool.getDeviceProviderKind().isSharedVolumeSupported())
        {
            boolean exclusive = requiresExclusiveLvLock(vlmDataRef, infoRef) ||
                !SharedStorageUtils.isNeededBySharedResource(vlmDataRef);
            lockMode = exclusive ? LvmLockMode.EXCLUSIVE : LvmLockMode.SHARED;
        }
        return lockMode;
    }

    /**
     * Whether pending snapshot work, existing snapshot LVs or a pending resize force the LV lock of
     * the given volume into exclusive mode even while another leg of the rsc-dfn shares the LV.
     */
    private boolean requiresExclusiveLvLock(LvmData<Resource> vlmDataRef, @Nullable LvsInfo infoRef)
    {
        Resource rsc = vlmDataRef.getRscLayerObject().getAbsResource();
        return !getCloneForKeyProps(rsc).isEmpty() ||
            hasLocalSnapshots(rsc) ||
            hasCachedSnapshotLvs(extractVolumeGroup(vlmDataRef), asLvIdentifier(vlmDataRef)) ||
            isResizePending(vlmDataRef, infoRef);
    }

    /**
     * Whether the LV of the given volume is going to be resized in this device manager run, based on
     * the "lvs" data the run works with. An LV smaller than its volume definition is grown regardless
     * of the RESIZE flag - this also covers a resize that was deferred while every copy of a shared
     * storage pool was INACTIVE and is applied by the next activation - while shrinking only happens
     * with the RESIZE flag set.
     */
    private boolean isResizePending(LvmData<Resource> vlmDataRef, @Nullable LvsInfo infoRef)
    {
        boolean pending = false;
        if (infoRef != null)
        {
            pending = getUsableSize(infoRef) < vlmDataRef.getExpectedSize() ||
                ((Volume) vlmDataRef.getVolume()).getFlags().isSet(Volume.Flags.RESIZE);
        }
        return pending;
    }

    private boolean hasLocalSnapshots(Resource rscRef)
    {
        boolean found = false;
        for (SnapshotDefinition snapDfn : rscRef.getResourceDefinition().getSnapshotDfns())
        {
            @Nullable Snapshot snap = snapDfn.getSnapshot(rscRef.getNode().getName());
            if (snap != null && !snap.isDeleted() && snap.getFlags().isUnset(Snapshot.Flags.DELETE))
            {
                found = true;
                break;
            }
        }
        return found;
    }

    /**
     * Whether the cached "lvs" data lists snapshot LVs of the given LV. Based on the possibly stale
     * {@link #infoListCache}; see {@link #hasThickSnapshots} for the variant confirming a hit with a
     * fresh query.
     */
    private boolean hasCachedSnapshotLvs(String volumeGroupRef, String lvmIdRef)
    {
        boolean found = false;
        for (LvsInfo info : infoListCache.values())
        {
            if (volumeGroupRef.equals(info.volumeGroup) && lvmIdRef.equals(info.origin))
            {
                found = true;
                break;
            }
        }
        return found;
    }

    /**
     * Every copy of a shared storage pool resource holds the snapshot objects, but only the node
     * with the active copy may keep the snapshot LV active: activating a thick snapshot implicitly
     * also activates its origin, interfering with the peer actively using the shared volume. The
     * same applies when this node holds no copy at all (e.g. it was deleted while the snapshot
     * remains).
     */
    private boolean isSnapshotLvBarredFromActivation(LvmData<Snapshot> snapVlmDataRef)
    {
        boolean barred = false;
        if (snapVlmDataRef.getStorPool().isShared())
        {
            Snapshot snap = snapVlmDataRef.getRscLayerObject().getAbsResource();
            @Nullable Resource localRsc = snap.getResourceDefinition().getResource(snap.getNodeName());
            barred = localRsc == null ||
                localRsc.getStateFlags().isSomeSet(
                    Resource.Flags.INACTIVE,
                    Resource.Flags.INACTIVE_PERMANENTLY
                );
        }
        return barred;
    }

    private void deactivateLv(String volumeGroupRef, String lvIdRef) throws StorageException
    {
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(volumeGroupRef),
            config -> LvmCommands.deactivateVolume(
                extCmdFactory.create(),
                volumeGroupRef,
                lvIdRef,
                config
            )
        );
        // deactivating also releases the lvmlockd LV lock
        appliedLockModes.remove(lockModeKey(volumeGroupRef, lvIdRef));
    }

    private String lockModeKey(String volumeGroupRef, String lvIdRef)
    {
        return volumeGroupRef + "/" + lvIdRef;
    }

    private void updateStripesPropIfNeeded(LvmData<?> vlmDataRef, @Nullable Integer stripesRef)
        throws DatabaseException
    {
        if (stripesRef != null && stripesRef > 1)
        {
            // only update if non-default
            Props props = StorageLayerSizeCalculator.getProps(vlmDataRef);
            String propKey = StorageLayerSizeCalculator.getStripesPropKey(vlmDataRef);
            @Nullable String propValue = props.getProp(propKey);
            String currentStripesStr = Integer.toString(stripesRef);
            if (propValue == null || !propValue.equals(currentStripesStr))
            {
                try
                {
                    props.setProp(propKey, currentStripesStr);
                }
                catch (InvalidKeyException | InvalidValueException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        }
    }

    protected String extractVolumeGroup(LvmData<?> vlmData)
    {
        return getVolumeGroup(vlmData.getStorPool());
    }

    @Override
    protected void createLvImpl(LvmData<Resource> vlmData)
        throws StorageException, DatabaseException
    {
        List<String> additionalOptions = ShellUtils.shellSplit(getLvcreateOptions(vlmData));
        Stream<String> additionalOptionsStream = additionalOptions.stream();
        List<String> pvSelection = ShellUtils.shellSplit(getLvcreatePvSelection(vlmData));
        if (!pvSelection.isEmpty())
        {
            additionalOptionsStream = Stream.concat(
                additionalOptionsStream,
                LvmUtils.getPhysicalVolumes(extCmdFactory, vlmData.getVolumeGroup(), pvSelection).stream()
            );
        }
        String[] additionalOptionsArr = additionalOptionsStream.toArray(String[]::new);

        if (additionalOptions.contains("--config"))
        {
            // no retry, use only users '--config' settings
            LvmCommands.createFat(
                extCmdFactory.create(),
                vlmData.getVolumeGroup(),
                asLvIdentifier(vlmData),
                vlmData.getExpectedSize(),
                null, // config is contained in additionalOptions
                additionalOptionsArr
            );
        }
        else
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.createFat(
                    extCmdFactory.create(),
                    vlmData.getVolumeGroup(),
                    asLvIdentifier(vlmData),
                    vlmData.getExpectedSize(),
                    config,
                    additionalOptionsArr
                )
            );
        }
        LvmUtils.recacheNext();

        updateStripesPropIfNeeded(vlmData, findStripesInAdditionalArgs(additionalOptionsArr));
    }

    protected String getLvCreateType(LvmData<Resource> vlmDataRef)
    {
        String type;
        try
        {
            type = getPrioProps(vlmDataRef)
                .getProp(
                    ApiConsts.KEY_STOR_POOL_LVCREATE_TYPE,
                    ApiConsts.NAMESPC_STORAGE_DRIVER,
                    DFLT_LVCREATE_TYPE
                );
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return type;
    }

    @SuppressWarnings("unchecked")
    protected PriorityProps getPrioProps(LvmData<?> vlmDataRef)
    {
        return vlmDataRef.getRscLayerObject().getAbsResource() instanceof Resource ?
            getPrioPropsRsc((LvmData<Resource>) vlmDataRef) :
            getPrioPropsSnap((LvmData<Snapshot>) vlmDataRef);
    }

    protected PriorityProps getPrioPropsRsc(LvmData<Resource> vlmDataRef)
    {
        Volume vlm = (Volume) vlmDataRef.getVolume();
        Resource rsc = vlm.getAbsResource();
        ResourceDefinition rscDfn = vlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        VolumeDefinition vlmDfn = vlm.getVolumeDefinition();
        return new PriorityProps(
            vlm.getProps(),
            rsc.getProps(),
            vlmDataRef.getStorPool().getProps(),
            rsc.getNode().getProps(),
            vlmDfn.getProps(),
            rscDfn.getProps(),
            rscGrp.getVolumeGroupProps(vlmDfn.getVolumeNumber()),
            rscGrp.getProps(),
            stltConfigAccessor.getReadonlyProps()
        );
    }

    protected PriorityProps getPrioPropsSnap(LvmData<Snapshot> vlmDataRef)
    {
        SnapshotVolume snapVlm = (SnapshotVolume) vlmDataRef.getVolume();
        Snapshot snap = snapVlm.getAbsResource();
        ResourceDefinition rscDfn = snapVlm.getResourceDefinition();
        ResourceGroup rscGrp = rscDfn.getResourceGroup();
        SnapshotVolumeDefinition snapVlmDfn = snapVlm.getSnapshotVolumeDefinition();
        SnapshotDefinition snapDfn = snap.getSnapshotDefinition();
        return new PriorityProps(
            snapVlm.getSnapVlmProps(),
            snapVlm.getVlmProps(),
            snap.getSnapProps(),
            snap.getRscProps(),
            vlmDataRef.getStorPool().getProps(),
            snap.getNode().getProps(),
            snapVlmDfn.getSnapVlmDfnProps(),
            snapVlmDfn.getVlmDfnProps(),
            snapDfn.getSnapDfnProps(),
            snapDfn.getRscDfnProps(),
            // we have to skip vlmDfn (not snapVlmDfn) since vlmDfn might have been removed in the meantime
            // we can still include rscDfn, since a rscDfn cannot be removed while it has snapshots
            rscDfn.getProps(),
            rscGrp.getVolumeGroupProps(snapVlmDfn.getVolumeNumber()),
            rscGrp.getProps(),
            stltConfigAccessor.getReadonlyProps()
        );
    }

    protected String getLvcreateOptions(LvmData<Resource> vlmDataRef)
    {
        return getProp(
            vlmDataRef,
            ApiConsts.NAMESPC_STORAGE_DRIVER,
            ApiConsts.KEY_STOR_POOL_LVCREATE_OPTIONS,
            ""
        );
    }

    protected String getLvcreatePvSelection(LvmData<Resource> vlmDataRef)
    {
        return getProp(
            vlmDataRef,
            ApiConsts.NAMESPC_STORAGE_DRIVER,
            ApiConsts.KEY_STOR_POOL_LVCREATE_PV_SELECTION,
            "--sort pv_used"
        );
    }

    protected String getLvcreateSnapshotOptions(LvmData<?> vlmDataRef)
    {
        return getProp(
            vlmDataRef,
            ApiConsts.NAMESPC_STORAGE_DRIVER,
            ApiConsts.KEY_STOR_POOL_LVCREATE_SNAPSHOT_OPTIONS,
            ""
        );
    }

    protected String getProp(LvmData<?> vlmDataRef, String namespace, String key, String dfltValue)
    {
        String options;
        try
        {
            options = getPrioProps(vlmDataRef).getProp(key, namespace, dfltValue);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return options;
    }

    @Override
    protected void resizeLvImpl(LvmData<Resource> vlmData)
        throws StorageException
    {
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmData.getVolumeGroup()),
            config -> LvmCommands.resize(
                extCmdFactory.create(),
                vlmData.getVolumeGroup(),
                asLvIdentifier(vlmData),
                vlmData.getExpectedSize(),
                config
            )
        );
        LvmUtils.recacheNext();
    }

    @Override
    protected void deleteLvImpl(LvmData<Resource> vlmData, String oldLvmId)
        throws StorageException, DatabaseException
    {
        /*
         * The devicePath can be null either when cleaning up a failed clone (cloning might still be set,
         * which prevents the devicePath from being set) or if the resource is inactive,
         * or maybe even if the LV (or VG) is inactive and therefore 'lvs'
         * simply does not return any device path for the given LV
         */
        @Nullable String devicePath = vlmData.getDevicePath();
        @Nullable String volumeGroup = vlmData.getVolumeGroup();

        // the LV gets deleted or renamed away - a possible later re-creation of the same LV id
        // starts with a fresh lvmlockd lock
        if (volumeGroup != null)
        {
            appliedLockModes.remove(lockModeKey(volumeGroup, oldLvmId));
        }

        if (volumeGroup != null && hasThickSnapshots(volumeGroup, oldLvmId))
        {
            /*
             * A thick snapshot cannot outlive its origin LV. Deleting the origin (lvremove -f) would also
             * delete all of its snapshots. Instead, rename the origin. The renamed LV is removed once its
             * last snapshot gets deleted.
             */
            String newLvmId = String.format(
                FORMAT_LVM_DELETED_ID,
                oldLvmId,
                TimeUtils.getRenameTime()
            );
            errorReporter.logInfo(
                "Lv %s/%s still has snapshots, renaming to %s instead of deleting",
                volumeGroup,
                oldLvmId,
                newLvmId
            );
            /*
             * The renamed LV (and its snapshots) must stay active here: deactivating a thick origin
             * also deactivates its snapshot LVs, and an in-progress rollback-via-restore still reads
             * from the snapshot device without anything reactivating it. Once the volume gets
             * deactivated on this node - it no longer holds the active copy of a shared storage
             * pool - deactivateRenamedOrigins releases the renamed origin together with its
             * snapshot LVs (and with them any lvmlockd locks).
             */
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(volumeGroup),
                config -> LvmCommands.rename(
                    extCmdFactory.create(),
                    volumeGroup,
                    oldLvmId,
                    newLvmId,
                    config
                )
            );
            vlmData.setExists(false);
            LvmUtils.recacheNextLvs();
        }
        else
        if (true)
        {
            if (devicePath != null)
            {
                wipeHandler.quickWipe(devicePath);
            }
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(volumeGroup),
                config -> LvmCommands.delete(
                    extCmdFactory.create(),
                    volumeGroup,
                    oldLvmId,
                    config,
                    LvmVolumeType.VOLUME
                )
            );
            vlmData.setExists(false);
            LvmUtils.recacheNext();
        }
        else
        {
            // TODO use this path once async wiping is implemented

            // devicePath is the "current" devicePath. as we will rename it right now
            // we will have to adjust the devicePath
            int lastIndexOf = devicePath.lastIndexOf(oldLvmId);

            // just make sure to not colide with any other ongoing wipe-lv-name
            String newLvmId = String.format(
                "%s-linstor_wiping_in_progress-%d",
                asLvIdentifier(vlmData),
                DELETED_ID.incrementAndGet()
            );
            devicePath = devicePath.substring(0, lastIndexOf) + newLvmId;

            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.rename(
                    extCmdFactory.create(),
                    volumeGroup,
                    oldLvmId,
                    newLvmId,
                    config
                )
            );
            LvmUtils.recacheNext();

            vlmData.setExists(false);

            wipeHandler.asyncWipe(
                devicePath,
                ignored ->
                {
                    LvmUtils.execWithRetry(
                        extCmdFactory,
                        Collections.singleton(vlmData.getVolumeGroup()),
                        config -> LvmCommands.delete(
                            extCmdFactory.create(),
                            volumeGroup,
                            newLvmId,
                            config,
                            LvmVolumeType.VOLUME
                        )
                    );
                    LvmUtils.recacheNext();
                }
            );
        }
    }

    @Override
    protected void deactivateLvImpl(LvmData<Resource> vlmDataRef, String ignoredLvIdRef)
        throws StorageException, DatabaseException
    {
        deactivateLv(vlmDataRef.getVolumeGroup(), vlmDataRef.getIdentifier());
        deactivateRenamedOrigins(vlmDataRef);
        LvmUtils.recacheNextLvs();
    }

    /**
     * Deactivates still active renamed ("_deleted_") origins of the given volume. The rename keeps
     * the LV - and implicitly its snapshot LVs, which deactivate together with their origin - active
     * on the node that deleted or restored the volume. That is legitimate while the node holds the
     * active copy of a shared storage pool, but once the volume is deactivated here the node must
     * release its device nodes and, on externally locked storage pools, its lvmlockd locks: they
     * would block the removal of the renamed LV together with its last snapshot from every other
     * node. {@link #updateInfo} counts active renamed origins as an active volume, so an INACTIVE
     * copy stays in the deactivation path until the leftovers are cleaned up.
     */
    private void deactivateRenamedOrigins(LvmData<Resource> vlmDataRef) throws StorageException
    {
        String volumeGroup = vlmDataRef.getVolumeGroup();
        for (String renamedOriginId : getActiveRenamedOrigins(vlmDataRef))
        {
            deactivateLv(volumeGroup, renamedOriginId);
        }
    }

    /**
     * The renamed ("_deleted_") origins of the given volume that are still active on this node,
     * based on the possibly stale cached "lvs" data. Only shared storage pools rename deleted or
     * restored origins, so anywhere else the result is empty.
     */
    private List<String> getActiveRenamedOrigins(LvmData<Resource> vlmDataRef)
    {
        List<String> activeRenamedOrigins = new ArrayList<>();
        if (vlmDataRef.getStorPool().isShared())
        {
            String volumeGroup = vlmDataRef.getVolumeGroup();
            String renamedPrefix = LVM_DELETED_PREFIX + vlmDataRef.getIdentifier() + "_";
            for (LvsInfo info : infoListCache.values())
            {
                if (volumeGroup.equals(info.volumeGroup) &&
                    isRenamedOrigin(renamedPrefix, info.identifier) &&
                    info.attributes.contains("a"))
                {
                    activeRenamedOrigins.add(info.identifier);
                }
            }
        }
        return activeRenamedOrigins;
    }

    /**
     * Whether the given LV is a renamed ("<code>_deleted_</code>") origin matching the given renamed
     * prefix ("<code>_deleted_&lt;lvId&gt;_</code>"). The trailing rename time is matched exactly:
     * with a prefix check alone a volume would also match the renamed origins of every volume whose
     * identifier merely extends its own (e.g. "web_00000" of resource "web" vs "web_00000_00000" of
     * resource "web_00000").
     */
    private boolean isRenamedOrigin(String renamedPrefixRef, String lvIdRef)
    {
        return lvIdRef.startsWith(renamedPrefixRef) &&
            LVM_DELETED_TIME_PATTERN.matcher(lvIdRef.substring(renamedPrefixRef.length())).matches();
    }

    @Override
    protected boolean snapshotExists(LvmData<Snapshot> snapVlmRef, boolean ignoredForTakeSnapshotRef)
        throws StorageException, DatabaseException
    {
        return infoListCache.get(getFullQualifiedIdentifier(snapVlmRef)) != null;
    }

    @Override
    protected void createSnapshot(LvmData<Resource> vlmDataRef, LvmData<Snapshot> snapVlmRef, boolean readOnly)
        throws StorageException, DatabaseException
    {
        List<String> additionalOptions = ShellUtils.shellSplit(getLvcreateSnapshotOptions(vlmDataRef));
        String[] additionalOptionsArr = new String[additionalOptions.size()];
        additionalOptions.toArray(additionalOptionsArr);

        /*
         * A thick snapshot needs a CoW area of a fixed size. Requesting the origin's size guarantees that
         * the snapshot can never become invalid, no matter how much data gets rewritten on the origin.
         * LVM itself caps the CoW size at the maximum useful size.
         */
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(vlmDataRef.getVolumeGroup()),
            config -> LvmCommands.createSnapshot(
                extCmdFactory.create(),
                readOnly,
                vlmDataRef.getVolumeGroup(),
                asLvIdentifier(vlmDataRef),
                asSnapLvIdentifier(snapVlmRef),
                config,
                vlmDataRef.getAllocatedSize(),
                additionalOptionsArr
            )
        );
        LvmUtils.recacheNextLvs();
    }

    @Override
    protected void deleteSnapshotImpl(LvmData<Snapshot> snapVlm)
        throws StorageException, DatabaseException
    {
        String volumeGroup = getVolumeGroup(snapVlm.getStorPool());
        @Nullable LvsInfo snapInfo = infoListCache.get(getFullQualifiedIdentifier(snapVlm));
        @Nullable String originLvId = snapInfo == null ? null : snapInfo.origin;

        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(snapVlm.getVolumeGroup()),
            config -> LvmCommands.delete(
                extCmdFactory.create(),
                volumeGroup,
                asSnapLvIdentifier(snapVlm),
                config,
                LvmVolumeType.SNAPSHOT
            )
        );
        snapVlm.setExists(false);
        LvmUtils.recacheNextLvs();

        if (originLvId != null && originLvId.startsWith(LVM_DELETED_PREFIX))
        {
            deleteOriginIfNoSnapshotLeft(volumeGroup, originLvId);
        }
    }

    /**
     * Removes the given renamed ("<code>_deleted_...</code>") origin LV if the just deleted snapshot was its
     * last one. See {@link #deleteLvImpl} for the renaming counterpart.
     */
    private void deleteOriginIfNoSnapshotLeft(String volumeGroupRef, String originLvIdRef)
        throws StorageException
    {
        @Nullable Map<String, LvsInfo> vgLvsInfo = LvmUtils.getLvsInfo(
            extCmdFactory,
            Collections.singleton(volumeGroupRef)
        ).get(volumeGroupRef);
        if (vgLvsInfo != null && vgLvsInfo.containsKey(originLvIdRef) && !hasSnapshotLvs(vgLvsInfo, originLvIdRef))
        {
            errorReporter.logInfo(
                "Removing %s/%s since its last snapshot was just deleted",
                volumeGroupRef,
                originLvIdRef
            );
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(volumeGroupRef),
                config -> LvmCommands.delete(
                    extCmdFactory.create(),
                    volumeGroupRef,
                    originLvIdRef,
                    config,
                    LvmVolumeType.VOLUME
                )
            );
            LvmUtils.recacheNextLvs();
        }
    }

    /**
     * Checks whether the given LV still has (thick) snapshots, based on a fresh "lvs" query if the cached
     * data indicates snapshots. The fresh query is needed since a snapshot found in the cache might have
     * been removed in the meantime (e.g. by the CloneService).
     */
    private boolean hasThickSnapshots(String volumeGroupRef, String lvmIdRef) throws StorageException
    {
        boolean hasSnapshots = hasCachedSnapshotLvs(volumeGroupRef, lvmIdRef);
        if (hasSnapshots)
        {
            LvmUtils.recacheNextLvs();
            @Nullable Map<String, LvsInfo> vgLvsInfo = LvmUtils.getLvsInfo(
                extCmdFactory,
                Collections.singleton(volumeGroupRef)
            ).get(volumeGroupRef);
            hasSnapshots = vgLvsInfo != null && hasSnapshotLvs(vgLvsInfo, lvmIdRef);
        }
        return hasSnapshots;
    }

    private boolean hasSnapshotLvs(Map<String, LvsInfo> vgLvsInfoRef, String lvmIdRef)
    {
        boolean hasSnapshotLvs = false;
        for (LvsInfo info : vgLvsInfoRef.values())
        {
            if (lvmIdRef.equals(info.origin))
            {
                hasSnapshotLvs = true;
                break;
            }
        }
        return hasSnapshotLvs;
    }

    @Override
    protected void restoreSnapshot(LvmData<Snapshot> sourceSnapVlmDataRef, LvmData<Resource> vlmDataRef)
        throws StorageException, DatabaseException
    {
        String volumeGroup = vlmDataRef.getVolumeGroup();
        String targetId = asLvIdentifier(vlmDataRef);
        String snapVolumeGroup = sourceSnapVlmDataRef.getVolumeGroup();
        String snapLvId = asSnapLvIdentifier(sourceSnapVlmDataRef);

        // a thick snapshot cannot be snapshotted again - restore by creating a new LV and copying the data
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(volumeGroup),
            config -> LvmCommands.createFat(
                extCmdFactory.create(),
                volumeGroup,
                targetId,
                vlmDataRef.getExpectedSize(),
                config
            )
        );
        LvmUtils.recacheNextLvs();

        /*
         * The source snapshot is not necessarily active: snapshots are created active, but
         * deactivating an inactive copy of a shared storage pool also deactivates its snapshot LVs
         * (they deactivate together with their renamed origin), and the restore dispatches never
         * contain the source snapshot, so updateInfo cannot reactivate it either. The implicit
         * activation of the renamed origin is legitimate: the node performing the restore holds the
         * active copy, and the next deactivation releases it again.
         */
        LvmUtils.execWithRetry(
            extCmdFactory,
            Collections.singleton(snapVolumeGroup),
            config -> LvmCommands.activateVolume(
                extCmdFactory.create(),
                snapVolumeGroup,
                snapLvId,
                config,
                LvmLockMode.DEFAULT
            )
        );
        LvmUtils.recacheNextLvs();

        String srcDevPath = getDevicePath(snapVolumeGroup, snapLvId);
        String tgtDevPath = getDevicePath(volumeGroup, targetId);
        waitUntilDeviceCreated(vlmDataRef, tgtDevPath);

        String blockSize = getProp(
            vlmDataRef,
            ApiConsts.NAMESPC_CLONE,
            ApiConsts.KEY_CLONE_DD_BLOCKSIZE,
            DFLT_RESTORE_DD_BLOCKSIZE
        );
        if (blockSize.isEmpty())
        {
            blockSize = DFLT_RESTORE_DD_BLOCKSIZE;
        }
        LvmCommands.copyDevice(extCmdFactory, srcDevPath, tgtDevPath, blockSize);
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> ret = new HashMap<>();
        Map<String, VgsInfo> vgsInfoMap = LvmUtils.getVgsInfo(
            extCmdFactory,
            changedStoragePoolStrings,
            false
        );
        for (String storPool : changedStoragePoolStrings)
        {
            @Nullable VgsInfo vgsInfo = vgsInfoMap.get(storPool);
            if (vgsInfo == null)
            {
                ret.put(storPool, SIZE_OF_NOT_FOUND_STOR_POOL);
            }
            else
            {
                ret.put(storPool, vgsInfo.vgFree);
            }
        }
        return ret;
    }

    @Override
    protected Map<String, LvsInfo> getInfoListImpl(
        List<LvmData<Resource>> vlmDataList,
        List<LvmData<Snapshot>> snapVlms
    )
        throws StorageException
    {
        if (hasSharedVolumeGroups(vlmDataList, snapVlms))
        {
            LvmCommands.vgscan(extCmdFactory.create(), true);
            // A shared volume group may have been modified by another node (e.g. an LV created or
            // removed on a different satellite). The 'vgscan' above only refreshes LVM's own metadata
            // cache; LINSTOR's LvsInfo/VgsInfo caches would still return the stale, pre-modification
            // view. Without this invalidation the following getLvsInfo() can miss an LV that another
            // node just created on the shared VG, making us try to (re)create it and fail with
            // "already exists" (see also getSpaceInfo, which reads the same caches).
            LvmUtils.recacheNext();
        }
        Map<String, Map<String, LvsInfo>> lvsInfoMap = LvmUtils.getLvsInfo(
            extCmdFactory,
            getAffectedVolumeGroups(vlmDataList, snapVlms)
        );
        HashMap<String /* fullQualfiedIdentifier */, LvsInfo> ret = new HashMap<>();
        for (Map<String, LvsInfo> lvMap : lvsInfoMap.values())
        {
            for (LvsInfo lvInfo : lvMap.values())
            {
                ret.put(lvInfo.volumeGroup + File.separator + lvInfo.identifier, lvInfo);
            }
        }
        return ret;
    }

    @Override
    public String getDevicePath(String storageName, String lvId)
    {
        return String.format("/dev/%s/%s", storageName, lvId);
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
            FORMAT_RSC_TO_LVM_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String asSnapLvIdentifier(LvmData<Snapshot> snapVlmDataRef)
    {
        StorageRscData<Snapshot> snapData = snapVlmDataRef.getRscLayerObject();
        return asSnapLvIdentifierRaw(
            snapData.getResourceName().displayValue,
            snapData.getResourceNameSuffix(),
            snapVlmDataRef.getVlmNr().value,
            snapData.getAbsResource().getSnapshotName().displayValue
        );
    }

    protected String asSnapLvIdentifierRaw(String rscNameRef, String rscNameSuffixRef, int vlmNrRef, String snapNameRef)
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
        return getVolumeGroup(storPoolRef);
    }

    protected @Nullable String getVolumeGroup(StorPoolInfo storPool)
    {
        String volumeGroup;
        try
        {
            volumeGroup = StringUtils.split(DeviceLayerUtils.getNamespaceStorDriver(storPool.getReadOnlyProps())
                .getProp(StorageConstants.CONFIG_LVM_VOLUME_GROUP_KEY), "/")[0];
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return volumeGroup;
    }

    @Override
    protected boolean updateDmStats()
    {
        return true; // LVM driver should call dmstats commands
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException
    {
        String vg = getVolumeGroup(storPool);
        if (vg == null)
        {
            throw new StorageException("Unset volume group for " + storPool);
        }
        @Nullable VgsInfo vgsInfo = LvmUtils.getVgsInfo(
            extCmdFactory,
            Collections.singleton(vg),
            false
        ).get(vg);

        @Nullable Long capacity = null;
        @Nullable Long freespace = null;
        if (vgsInfo != null)
        {
            capacity = vgsInfo.vgSize;
            freespace = vgsInfo.vgFree;
        }
        return SpaceInfo.buildOrThrowOnError(capacity, freespace, storPool);
    }

    /*
     * Expected to be overridden by LvmThinProvider (maybe additionally called)
     */
    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPool)
        throws StorageException
    {
        ReadOnlyProps publicStorDriverNamespace = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getReadOnlyProps()
        );
        StorageConfigReader.checkVolumeGroupEntry(extCmdFactory, publicStorDriverNamespace);
        StorageConfigReader.checkToleranceFactor(publicStorDriverNamespace);

        return null;
    }

    protected void checkExtentSize(StorPoolInfo storPool, LocalPropsChangePojo ret)
        throws StorageException, ImplementationError
    {
        String lvmVG = getVolumeGroup(storPool);
        Map<String, VgsInfo> extentSizeInKibMap = LvmUtils.getVgsInfo(
            extCmdFactory,
            Collections.singleton(lvmVG),
            false
        );
        @Nullable VgsInfo vgsInfo = extentSizeInKibMap.get(lvmVG);
        if (vgsInfo != null)
        {
            markAllocGranAsChangedIfNeeded(vgsInfo.vgExtentSize, storPool, ret);
        }
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPoolRef)
        throws DatabaseException, StorageException
    {
        LocalPropsChangePojo ret = new LocalPropsChangePojo();
        List<String> pvs = LvmUtils.getPhysicalVolumes(extCmdFactory, getVolumeGroup(storPoolRef));
        if (PmemUtils.supportsDax(extCmdFactory.create(), pvs))
        {
            storPoolRef.setPmem(true);
        }
        checkExtentSize(storPoolRef, ret);

        return ret;
    }

    private Set<String> getAffectedVolumeGroups(
        Collection<LvmData<Resource>> vlmDataList,
        Collection<LvmData<Snapshot>> snapVlms
    )
    {
        ArrayList<LvmData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlms);

        Set<String> volumeGroups = new HashSet<>();
        for (LvmData<?> vlmData : combinedList)
        {
            String volumeGroup = vlmData.getVolumeGroup();
            if (volumeGroup == null)
            {
                volumeGroup = getVolumeGroup(vlmData.getStorPool());
                vlmData.setVolumeGroup(volumeGroup);
            }
            if (volumeGroup != null)
            {
                volumeGroups.add(volumeGroup);
            }
        }
        return volumeGroups;
    }

    private boolean hasSharedVolumeGroups(
        Collection<LvmData<Resource>> vlmDataList,
        Collection<LvmData<Snapshot>> snapVlms
    )
    {
        boolean ret = false;
        ArrayList<LvmData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(vlmDataList);
        combinedList.addAll(snapVlms);

        for (LvmData<?> vlmData : combinedList)
        {
            StorPool storPool = vlmData.getStorPool();
            if (storPool.isShared())
            {
                ret = true;
                break;
            }
        }
        return ret;
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return true;
    }

    @Override
    protected void setDevicePath(LvmData<Resource> vlmData, String devPath) throws DatabaseException
    {
        vlmData.setDevicePath(devPath);
    }

    @Override
    protected void createSnapshotForCloneImpl(
        LvmData<Resource> vlmData,
        String cloneRscName)
        throws StorageException, DatabaseException
    {
        final String srcId = asLvIdentifier(vlmData);
        final String srcFullSnapshotName = getCloneSnapshotNameFull(vlmData, cloneRscName, "_");

        if (!infoListCache.containsKey(vlmData.getVolumeGroup() + "/" + srcFullSnapshotName))
        {
            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.createSnapshot(
                    extCmdFactory.create(),
                    false,
                    vlmData.getVolumeGroup(),
                    srcId,
                    srcFullSnapshotName,
                    config,
                    vlmData.getAllocatedSize()
                )
            );

            LvmUtils.execWithRetry(
                extCmdFactory,
                Collections.singleton(vlmData.getVolumeGroup()),
                config -> LvmCommands.addTag(
                    extCmdFactory.create(),
                    vlmData.getVolumeGroup(),
                    srcFullSnapshotName,
                    LvmCommands.LVM_TAG_CLONE_SNAPSHOT,
                    config
                )
            );
            LvmUtils.recacheNextLvs();
        }
        else
        {
            errorReporter.logInfo("Clone base snapshot %s already found, reusing.", srcFullSnapshotName);
        }
    }

    @Override
    protected void setAllocatedSize(LvmData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(LvmData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(LvmData<Resource> vlmData, long size)
    {
        vlmData.setExpectedSize(size);
    }

    @Override
    protected String getStorageName(LvmData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getVolumeGroup();
    }

    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef) throws StorageException
    {
        String vlmGrp = getVolumeGroup(vlmDataRef.getStorPool());
        Map<String, VgsInfo> vgs = LvmUtils.getVgsInfo(
            extCmdFactory,
            Collections.singleton(vlmGrp),
            false
        );
        @Nullable VgsInfo vgsInfo = vgs.get(vlmGrp);
        long extentSize;
        if (vgsInfo != null)
        {
            extentSize = vgsInfo.vgExtentSize;
        }
        else
        {
            throw new StorageException("VolumeGroup " + vlmGrp + " not found");
        }
        return extentSize;
    }

    @Override
    protected int getExtentSizeMulFactor(VlmProviderObject<?> vlmDataRef)
    {
        return getStripeCount(vlmDataRef);
    }

    private int getStripeCount(VlmProviderObject<?> vlmDataRef)
    {
        @Nullable Integer stripeCount = null;
        @SuppressWarnings("unchecked")
        LvmData<Resource> lvmData = (LvmData<Resource>) vlmDataRef;
        if (vlmDataRef.exists())
        {
            @Nullable LvsInfo lvsInfo = infoListCache.get(getFullQualifiedIdentifier(lvmData));
            if (lvsInfo != null)
            {
                stripeCount = lvsInfo.stripes;
            }
        }
        if (stripeCount == null)
        {
            List<String> additionalOptions = ShellUtils.shellSplit(getLvcreateOptions(lvmData));
            String[] additionalOptionsArr = new String[additionalOptions.size()];
            additionalOptions.toArray(additionalOptionsArr);
            stripeCount = findStripesInAdditionalArgs(additionalOptionsArr);
        }
        if (stripeCount == null)
        {
            stripeCount = DFLT_STRIPES;
        }
        return stripeCount;
    }

    private @Nullable Integer findStripesInAdditionalArgs(String[] additionalOptionsArr)
    {
        @Nullable Integer stripeCount = null;
        try
        {
            for (int idx = 0; idx < additionalOptionsArr.length; idx++)
            {
                String arg = additionalOptionsArr[idx];
                if (arg.equals("-i") || arg.equals("--stripes"))
                {
                    stripeCount = Integer.parseInt(additionalOptionsArr[idx + 1]);
                    break;
                }
            }
        }
        catch (NumberFormatException nfe)
        {
            errorReporter.reportError(nfe);
        }
        return stripeCount;
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }

    @Override
    public void openForClone(VlmProviderObject<?> vlm, @Nullable String cloneName, boolean readOnly)
        throws StorageException, DatabaseException
    {
        LvmData<Resource> srcData = (LvmData<Resource>) vlm;
        if (cloneName != null)
        {
            // use snapshot path
            vlm.setCloneDevicePath(getDevicePath(
                srcData.getVolumeGroup(), getCloneSnapshotNameFull(srcData, cloneName, "_")));
        }
        else
        {
            createLvImpl(srcData);
            String devicePath = getDevicePath(srcData.getVolumeGroup(), asLvIdentifier(srcData));
            waitUntilDeviceCreated(srcData, devicePath);
            vlm.setCloneDevicePath(devicePath);
        }
    }

    @Override
    public void closeForClone(VlmProviderObject<?> vlm, @Nullable String cloneName) throws StorageException
    {
        vlm.setCloneDevicePath(null);
    }

    @Override
    protected @Nullable String getReadOnlyProbeDevice(final StorPool storPoolRef) throws StorageException
    {
        // a thick LV is a linear mapping onto the volume group's PVs, so its queue limits (min/opt IO
        // size, discard granularity) are those of the underlying PV: read them from the PV instead of
        // creating a temporary probe LV. Creating an LV writes VG metadata, which corrupts a shared VG
        // when multiple satellites probe concurrently (the probe holds no shared-space lock).
        final List<String> physicalVolumes = LvmUtils.getPhysicalVolumes(
            extCmdFactory,
            getStorageName(storPoolRef)
        );
        return physicalVolumes.isEmpty() ? null : physicalVolumes.get(0);
    }
}
