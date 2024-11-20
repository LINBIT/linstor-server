package com.linbit.linstor.layer.storage.file;

import com.linbit.ImplementationError;
import com.linbit.PlatformStlt;
import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactoryStlt;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.api.SpaceInfo;
import com.linbit.linstor.backupshipping.BackupShippingMgr;
import com.linbit.linstor.clone.CloneService;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.apicallhandler.StltExtToolsChecker;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.interfaces.StorPoolInfo;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.DeviceLayerUtils;
import com.linbit.linstor.layer.storage.AbsStorageProvider;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.file.utils.FileCommands;
import com.linbit.linstor.layer.storage.file.utils.FileProviderUtils;
import com.linbit.linstor.layer.storage.file.utils.FileProviderUtils.FileInfo;
import com.linbit.linstor.layer.storage.file.utils.LosetupCommands;
import com.linbit.linstor.layer.storage.utils.PmemUtils;
import com.linbit.linstor.layer.storage.utils.StltProviderUtils;
import com.linbit.linstor.layer.storage.utils.StorageConfigReader;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.snapshotshipping.SnapshotShippingService;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.StorageRscData;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.TransactionException;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

@Singleton
public class FileProvider extends AbsStorageProvider<FileInfo, FileData<Resource>, FileData<Snapshot>>
{
    private static final String DUMMY_LINSTOR_TEST_SOURCE = "LinstorSnapshotTestSource.img";
    private static final String DUMMY_LINSTOR_TEST_TARGET = "LinstorSnapshotTestTarget.img";

    private static final String FORMAT_VLM_TO_ID_BASE = "%s%s_%05d";
    private static final String FORMAT_VLM_TO_ID = FORMAT_VLM_TO_ID_BASE + ".img";
    private static final String FORMAT_SNAP_VLM_TO_ID = FORMAT_VLM_TO_ID_BASE + "_%s.img";

    private static final String FORMAT_ID_WIPE_IN_PROGRESS = "%s_linstor_wiping_in_progress";

    private static final String LODEV_FILE = LinStor.CONFIG_PATH + "/loop_device_mapping";
    private static final String LODEV_FILE_TMP = LODEV_FILE + ".tmp";

    private static final Map<String, String> LOSETUP_DEVICES = new TreeMap<>();

    private final PlatformStlt platformStlt;

    protected FileProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        String subTypeDescr,
        DeviceProviderKind subTypeKind,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef,
        PlatformStlt platformStltRef,
        FileSystemWatch fileSystemWatchRef
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
            subTypeDescr,
            subTypeKind,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef,
            fileSystemWatchRef
        );
        platformStlt = platformStltRef;
    }

    @Inject
    public FileProvider(
        ErrorReporter errorReporter,
        ExtCmdFactoryStlt extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        SnapshotShippingService snapShipMrgRef,
        StltExtToolsChecker extToolsCheckerRef,
        CloneService cloneServiceRef,
        BackupShippingMgr backupShipMgrRef,
        PlatformStlt platformStltRef,
        FileSystemWatch fileSystemWatchRef
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
            "FILE",
            DeviceProviderKind.FILE,
            snapShipMrgRef,
            extToolsCheckerRef,
            cloneServiceRef,
            backupShipMgrRef,
            fileSystemWatchRef
        );
        platformStlt = platformStltRef;
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.FILE;
    }

    @Override
    protected void updateStates(List<FileData<Resource>> fileDataList, List<FileData<Snapshot>> snapshots)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        List<FileData<?>> combinedList = new ArrayList<>();
        combinedList.addAll(fileDataList);
        combinedList.addAll(snapshots);

        for (FileData<?> fileData : combinedList)
        {
            final FileInfo fileInfo = infoListCache.get(getFullQualifiedIdentifier(fileData));
            updateInfo(fileData, fileInfo);

            if (fileInfo != null)
            {
                final long expectedSize = fileData.getExpectedSize();
                final long actualSize = fileInfo.size;
                if (actualSize != expectedSize)
                {
                    if (actualSize < expectedSize)
                    {
                        fileData.setSizeState(Size.TOO_SMALL);
                    }
                    else
                    {
                        fileData.setSizeState(Size.TOO_LARGE);
                    }
                }
                else
                {
                    fileData.setSizeState(Size.AS_EXPECTED);
                }
            }
        }
    }

    protected String getFullQualifiedIdentifier(FileData<?> fileData)
    {
        Path storageDirectory = fileData.getStorageDirectory();
        if (storageDirectory == null)
        {
            String storDirStr;
            try
            {
                storDirStr = DeviceLayerUtils.getNamespaceStorDriver(
                    fileData.getStorPool().getProps(storDriverAccCtx)
                )
                    .getProp(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);
            }
            catch (AccessDeniedException | InvalidKeyException exc)
            {
                throw new ImplementationError(exc);
            }
            storageDirectory = Paths.get(storDirStr);
            fileData.setStorageDirectory(storageDirectory);
        }
        String identifier = getIdentifier(fileData);
        return storageDirectory.resolve(identifier).toString();
    }

    @SuppressWarnings("unchecked")
    private String getIdentifier(FileData<?> fileData)
    {
        String identifier;
        if (fileData.getVolume() instanceof Volume)
        {
            identifier = asLvIdentifier((FileData<Resource>) fileData);
        }
        else
        {
            identifier = asSnapLvIdentifier((FileData<Snapshot>) fileData);
        }
        return identifier;
    }

    /*
     * Might be overridden (extended) by future *Providers
     */
    protected void updateInfo(FileData<?> fileData, FileInfo info)
        throws DatabaseException, StorageException
    {
        fileData.setIdentifier(getIdentifier(fileData));
        if (info == null)
        {
            fileData.setExists(false);
            fileData.setStorageDirectory(extractStorageDirectory(fileData));
            fileData.setDevicePath(null);
            fileData.setAllocatedSize(-1);
            fileData.setUsableSize(-1);
        }
        else
        {
            fileData.setExists(true);
            fileData.setStorageDirectory(info.directory);
            if (info.loPath != null)
            {
                fileData.setDevicePath(info.loPath.toString());
            }
            else
            {
                // snapshot
                fileData.setDevicePath(null);
            }
            fileData.setIdentifier(info.identifier);
            fileData.setAllocatedSize(info.size);
            fileData.setUsableSize(info.size);
        }
    }

    protected Path extractStorageDirectory(FileData<?> fileData) throws StorageException
    {
        return getStorageDirectory(fileData.getStorPool());
    }

    @Override
    protected void createLvImpl(FileData<Resource> fileData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Path backingFile = fileData.getStorageDirectory().resolve(fileData.getIdentifier());
        FileCommands.createFat(
            extCmdFactory.create(),
            backingFile,
            fileData.getExpectedSize()
        );
        createLoopDevice(fileData, backingFile);
    }

    protected void createLoopDevice(FileData<Resource> fileData, Path backingFile)
        throws StorageException, DatabaseException
    {
        String loDev = new String(
            LosetupCommands.attach(
                extCmdFactory.create(),
                backingFile
            )
            .stdoutData).trim();

        LOSETUP_DEVICES.put(loDev, backingFile.toString());
        fileData.setDevicePath(loDev);
    }

    @Override
    protected void resizeLvImpl(FileData<Resource> fileData)
        throws StorageException, AccessDeniedException
    {
        // no special command for resize, just "re-allocate" to the needed size
        FileCommands.createFat(
            extCmdFactory.create(),
            fileData.getStorageDirectory().resolve(fileData.getIdentifier()),
            fileData.getExpectedSize()
        );
        LosetupCommands.resize(
            extCmdFactory.create(),
            fileData.getDevicePath()
        );
    }

    @Override
    protected void deleteLvImpl(FileData<Resource> fileData, String oldId)
        throws StorageException, DatabaseException
    {
        String devicePath = fileData.getDevicePath();
        Path storageDirectory = fileData.getStorageDirectory();

        if (true)
        {
            wipeHandler.quickWipe(devicePath);
            errorReporter.logDebug(
                "Detaching %s volume %s/%s (device: %s)",
                kind.toString(),
                storageDirectory,
                oldId,
                devicePath
            );
            LosetupCommands.detach(extCmdFactory.create(), devicePath);
            errorReporter.logDebug("Deleting %s volume %s/%s", kind.toString(), storageDirectory, oldId);
            FileCommands.delete(
                storageDirectory,
                oldId
            );
            fileData.setExists(false);
        }
        else
        {
            // TODO use this path once async wiping is implemened

            // just make sure to not colide with any other ongoing wipe-lv-name
            String newId = String.format(FORMAT_ID_WIPE_IN_PROGRESS, UUID.randomUUID().toString());
            FileCommands.rename(
                storageDirectory,
                oldId,
                newId
            );

            wipeHandler.asyncWipe(
                devicePath,
                ignored ->
                {
                    try
                    {
                        LosetupCommands.detach(extCmdFactory.create(), devicePath);
                        FileCommands.delete(
                            storageDirectory,
                            newId
                        );
                        fileData.setExists(false);
                    }
                    catch (DatabaseException exc)
                    {
                        throw new ImplementationError(exc);
                    }
                }
            );
        }

        LOSETUP_DEVICES.remove(devicePath);
    }

    @Override
    protected void deactivateLvImpl(FileData<Resource> vlmDataRef, String lvIdRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // noop, not supported
    }

    @Override
    protected void createSnapshot(FileData<Resource> fileData, FileData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Path storageDirectory = fileData.getStorageDirectory();
        FileCommands.createSnapshot(
            extCmdFactory.create(),
            storageDirectory.resolve(fileData.getIdentifier()),
            storageDirectory.resolve(snapVlmRef.getIdentifier())
        );
    }

    @Override
    protected void restoreSnapshot(FileData<Snapshot> sourceSnapVlmDataRef, FileData<Resource> fileDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {

        Path storageDirectory = fileDataRef.getStorageDirectory();
        Path devPath = storageDirectory.resolve(fileDataRef.getIdentifier());
        FileCommands.copy(
            extCmdFactory.create(),
            asFullQualifiedPath(sourceSnapVlmDataRef),
            devPath
        );
        createLoopDevice(fileDataRef, devPath);
    }

    @Override
    protected void deleteSnapshotImpl(FileData<Snapshot> snapVlmRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Path storageDirectory = getStorageDirectory(snapVlmRef.getStorPool());
        String asSnapLvIdentifier = asSnapLvIdentifier(snapVlmRef);
        errorReporter.logDebug(
            "Deleting %s snapshot-volume %s/%s",
            kind.toString(),
            storageDirectory,
            asSnapLvIdentifier
        );
        FileCommands.delete(storageDirectory, asSnapLvIdentifier);
    }

    @Override
    protected boolean snapshotExists(FileData<Snapshot> snapVlmRef, boolean ignoredForTakeSnapshorRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        return Files.exists(asFullQualifiedPath(snapVlmRef));
    }

    @Override
    protected void rollbackImpl(FileData<Resource> fileData, FileData<Snapshot> rollbackToSnapVlmDataRef)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Path storageDirectory = fileData.getStorageDirectory();
        Path snapPath = asFullQualifiedPath(rollbackToSnapVlmDataRef);
        // do not use java.nio.file.Files.copy ! that seems to do something different (and does not work
        // for this method :) )
        FileCommands.copy(
            extCmdFactory.create(),
            snapPath,
            storageDirectory.resolve(fileData.getIdentifier())
        );
    }

    @Override
    protected String asSnapLvIdentifier(FileData<Snapshot> snapVlmDataRef)
    {
        StorageRscData<Snapshot> snapData = snapVlmDataRef.getRscLayerObject();
        return asSnapLvIdentifierRaw(
            snapData.getResourceName().displayValue,
            snapData.getResourceNameSuffix(),
            snapVlmDataRef.getVlmNr().value,
            snapData.getAbsResource().getSnapshotName().displayValue
        );
    }

    private String asSnapLvIdentifierRaw(String rscNameRef, String rscNameSuffixRef, int vlmNrRef, String snapNameRef)
    {
        return String.format(
            FORMAT_SNAP_VLM_TO_ID,
            rscNameRef,
            rscNameSuffixRef,
            vlmNrRef,
            snapNameRef
        );
    }

    private Path asFullQualifiedPath(FileData<Snapshot> snapVlmDataRef)
        throws StorageException
    {
        Path storageDirectory = getStorageDirectory(snapVlmDataRef.getStorPool());
        return storageDirectory.resolve(asSnapLvIdentifier(snapVlmDataRef));
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSizes = FileProviderUtils.getDirFreeSizes(changedStoragePoolStrings);
        for (String dir : changedStoragePoolStrings)
        {
            if (!freeSizes.containsKey(dir))
            {
                freeSizes.put(dir, SIZE_OF_NOT_FOUND_STOR_POOL);
            }
        }
        return freeSizes;
    }

    @Override
    protected Map<String, FileInfo> getInfoListImpl(
        List<FileData<Resource>> fileDataList,
        List<FileData<Snapshot>> snapVlmDataList
    )
        throws StorageException, AccessDeniedException, DatabaseException
    {
        // It is possible that the backing file still exists for a logical volume, but the loop-device does not

        // Map<path in FS, linstorData> - only for resources, no snapshots
        Map<String, FileData<Resource>> backingFileToFileDataMap = new HashMap<>();
        for (FileData<Resource> fileData : fileDataList)
        {
            backingFileToFileDataMap.put(
                getFullQualifiedIdentifier(fileData),
                fileData
            );
        }

        // queries losetup, not ls
        Map<String, FileInfo> infoList = FileProviderUtils.getInfoList(
            extCmdFactory.create(),
            path -> getAllocatedSizeFileImpl(extCmdFactory.create(), path)
        );
        for (Entry<String, FileInfo> entry : infoList.entrySet())
        {
            String backingFile = entry.getKey();
            // for every resource we found via losetup, delete its entry from this map
            FileData<Resource> fileData = backingFileToFileDataMap.remove(backingFile);

            LOSETUP_DEVICES.put(entry.getValue().loPath.toString(), backingFile);
        }

        // for every entry left in this map, check if the file exists and if so, create a new loop device
        for (Entry<String, FileData<Resource>> entry : backingFileToFileDataMap.entrySet())
        {
            FileData<Resource> fileData = entry.getValue();
            Path backingFile = Paths.get(entry.getKey());

            if (Files.exists(backingFile))
            {
                createLoopDevice(fileData, backingFile);
                infoList.put(
                    backingFile.toString(),
                    new FileInfo(
                        Paths.get(fileData.getDevicePath()),
                        backingFile
                    )
                );
            }
        }
        for (FileData<Snapshot> snapVlmData : snapVlmDataList)
        {
            Path snapPath = Paths.get(getFullQualifiedIdentifier(snapVlmData));
            if (Files.exists(snapPath))
            {
                infoList.put(
                    snapPath.toString(),
                    new FileInfo(
                        null,
                        snapPath
                    )
                );
            }
        }

        return infoList;
    }

    protected long getAllocatedSizeFileImpl(ExtCmd extCmd, String pathRef) throws StorageException
    {
        return StltProviderUtils.getAllocatedSize(pathRef, extCmd);
    }

    @Override
    public String getDevicePath(String storageName, String lvId)
    {
        return null; // we cannot construct it from the given data. however, the devicePath is already
        // set on the corresponding FileData object
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
            FORMAT_VLM_TO_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String getStorageName(StorPool storPoolRef) throws StorageException
    {
        return getStorageDirectory(storPoolRef).toString();
    }

    protected Path getStorageDirectory(StorPoolInfo storPool) throws StorageException
    {
        Path dir;
        try
        {
            String dirStr = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getReadOnlyProps(storDriverAccCtx)
            )
                .getProp(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);
            if (dirStr != null)
            {
                dir = Paths.get(dirStr);
            }
            else
            {
                throw new StorageException("Unset storage directory for " + storPool);
            }
        }
        catch (InvalidKeyException | AccessDeniedException exc)
        {
            throw new ImplementationError(exc);
        }
        return dir;
    }

    @Override
    protected boolean updateDmStats()
    {
        return false;
    }

    @Override
    public SpaceInfo getSpaceInfo(StorPoolInfo storPool) throws StorageException, AccessDeniedException
    {
        Path dir = getStorageDirectory(storPool);
        long capacity = FileProviderUtils.getPoolCapacity(extCmdFactory.create(), dir);
        long freeSpace = FileProviderUtils.getFreeSpace(extCmdFactory.create(), dir);
        return SpaceInfo.buildOrThrowOnError(capacity, freeSpace, storPool);
    }

    @Override
    protected boolean waitForSnapshotDevice()
    {
        return true;
    }

    @Override
    public @Nullable LocalPropsChangePojo checkConfig(StorPoolInfo storPool)
        throws StorageException, AccessDeniedException
    {
        ReadOnlyProps props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getReadOnlyProps(storDriverAccCtx)
        );
        StorageConfigReader.checkFileStorageDirectoryEntry(props);

        return null;
    }

    @Override
    public @Nullable LocalPropsChangePojo update(StorPool storPool)
        throws AccessDeniedException, DatabaseException, StorageException
    {
        ReadOnlyProps props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getProps(storDriverAccCtx)
        );
        String dirStr = props.getProp(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);
        Path storageDirectory = Paths.get(dirStr);

        String dev = FileProviderUtils.getSourceDevice(extCmdFactory.create(), storageDirectory);
        if (PmemUtils.supportsDax(extCmdFactory.create(), dev))
        {
            storPool.setPmem(true);
        }

        updateSnapshotCapabilitiesIfNeeded(storPool, props);
        return null;
    }

    private void updateSnapshotCapabilitiesIfNeeded(StorPool storPool, ReadOnlyProps props)
        throws StorageException, ImplementationError, TransactionException, AccessDeniedException
    {
        // try to create a dummy snapshot to verify if we can
        if (!storPool.isSnapshotSupportedInitialized(storDriverAccCtx))
        {
            try
            {
                String dirStr = props.getProp(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);
                Path storageDirectory = Paths.get(dirStr);
                Path dummyVlmPath = storageDirectory.resolve(DUMMY_LINSTOR_TEST_SOURCE);
                Path dummyTargetPath = storageDirectory.resolve(Paths.get(DUMMY_LINSTOR_TEST_TARGET));
                try
                {
                    dummyVlmPath.toFile().createNewFile();
                }
                catch (IOException exc)
                {
                    throw new StorageException("Cannot create dummy file in given storage directory '" +
                        dummyVlmPath + "'", exc);
                }

                try
                {
                    FileCommands.createSnapshot(
                        extCmdFactory.create(),
                        dummyVlmPath,
                        dummyTargetPath
                    );
                    storPool.setSupportsSnapshot(storDriverAccCtx, true);
                }
                catch (StorageException ignored)
                {
                    storPool.setSupportsSnapshot(storDriverAccCtx, false);
                }
                finally
                {
                    if (Files.exists(dummyTargetPath))
                    {
                        dummyTargetPath.toFile().delete();
                    }
                    if (Files.exists(dummyVlmPath))
                    {
                        dummyVlmPath.toFile().delete();
                    }

                    transMgrProvider.get().commit();
                }
            }
            catch (DatabaseException | InvalidKeyException exc)
            {
                throw new ImplementationError(exc);
            }
        }
    }

    @Override
    public void clearCache() throws StorageException
    {
        StringBuilder sb = new StringBuilder();
        String tmpName, lodevName;

        for (Entry<String, String> entry : LOSETUP_DEVICES.entrySet())
        {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
        }

        tmpName = platformStlt.sysRoot() + LODEV_FILE_TMP;
        lodevName = platformStlt.sysRoot() + LODEV_FILE;

        File tmp = new File(tmpName);
        try (FileOutputStream fos = new FileOutputStream(tmp))
        {
            fos.write(sb.toString().getBytes());
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to write active loopback devices to " + tmpName, exc);
        }

        try
        {
            Files.move(
                tmp.toPath(),
                new File(lodevName).toPath(),
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE
            );
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to move " + tmpName + " to " + lodevName, exc);
        }

        super.clearCache();
    }

    @Override
    protected void setDevicePath(FileData<Resource> vlmData, String devPath) throws DatabaseException
    {
        // ignored - devicePath is set when creating or when looking for fileData objects
    }

    @Override
    protected long getExtentSize(AbsStorageVlmData<?> vlmDataRef)
    {
        /*
         * losetup wants to round to 512 bytes (1 sector), but linstor calculates in KiB
         */
        return 1;
    }

    @Override
    protected void setAllocatedSize(FileData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(FileData<Resource> vlmData, long size) throws DatabaseException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(FileData<Resource> vlmData, long size)
    {
        vlmData.setExpectedSize(size);
    }

    @Override
    protected String getStorageName(FileData<Resource> vlmDataRef) throws DatabaseException
    {
        return vlmDataRef.getStorageDirectory().toString();
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        return fetchOrigAllocatedSizes(vlmDataListRef);
    }
}
