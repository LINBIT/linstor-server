package com.linbit.linstor.storage.layer.provider.file;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.SnapshotVolume;
import com.linbit.linstor.StorPool;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject.Size;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.AbsStorageProvider;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.layer.provider.utils.StorageConfigReader;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.FileCommands;
import com.linbit.linstor.storage.utils.FileUtils;
import com.linbit.linstor.storage.utils.LosetupCommands;
import com.linbit.linstor.storage.utils.FileUtils.FileInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;

import com.google.common.io.Files;

@Singleton
public class FileProvider extends AbsStorageProvider<FileInfo, FileData>
{
    private static final String FORMAT_RSC_TO_ID = "%s%s_%05d.img";
    private static final String FORMAT_ID_WIPE_IN_PROGRESS = "%s_linstor_wiping_in_progress";

    private static final String LODEV_FILE = "/var/lib/linstor/losetup.dmp";
    private static final String LODEV_FILE_TMP = LODEV_FILE + ".tmp";

    private final Map<String, String> loDevs = new TreeMap<>();

    protected FileProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider,
        String subTypeDescr,
        DeviceProviderKind subTypeKind
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
            subTypeKind
        );
    }

    @Inject
    public FileProvider(
        ErrorReporter errorReporter,
        ExtCmdFactory extCmdFactory,
        @DeviceManagerContext AccessContext storDriverAccCtx,
        StltConfigAccessor stltConfigAccessor,
        WipeHandler wipeHandler,
        Provider<NotificationListener> notificationListenerProvider,
        Provider<TransactionMgr> transMgrProvider
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
            DeviceProviderKind.FILE
        );
    }

    @Override
    protected void updateStates(List<FileData> fileDataList, Collection<SnapshotVolume> snapshots)
        throws StorageException, AccessDeniedException, SQLException
    {
        for (FileData fileData : fileDataList)
        {
            final FileInfo fileInfo = infoListCache.get(getFullQualifiedIdentifier(fileData));
            updateInfo(fileData, fileInfo);

            if (fileInfo != null)
            {
                final long expectedSize = fileData.getExepectedSize();
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
        updateSnapshotStates(snapshots);
    }

    private String getFullQualifiedIdentifier(FileData fileData)
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
        return storageDirectory.resolve(asLvIdentifier(fileData)).toString();
    }

    /*
     * Might be overridden (extended) by future *Providers
     */
    protected void updateInfo(FileData fileData, FileInfo info)
        throws SQLException
    {
        fileData.setIdentifier(asLvIdentifier(fileData));
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
            fileData.setDevicePath(info.loPath.toString());
            fileData.setIdentifier(info.identifier);
            fileData.setAllocatedSize(info.size);
            fileData.setUsableSize(info.size);
        }
    }

    protected Path extractStorageDirectory(FileData fileData)
    {
        return getStorageDirectory(fileData.getStorPool());
    }

    /*
     * Expected to be overridden by LvmThinProvider
     */
    @SuppressWarnings("unused")
    protected void updateSnapshotStates(Collection<SnapshotVolume> snapshots)
        throws AccessDeniedException, SQLException
    {
        // no-op
    }

    @Override
    protected void createLvImpl(FileData fileData)
        throws StorageException, AccessDeniedException, SQLException
    {
        Path backingFile = fileData.getStorageDirectory().resolve(fileData.getIdentifier());
        if (fileData.getProviderKind().usesThinProvisioning())
        {
            FileCommands.createThin(
                extCmdFactory.create(),
                backingFile,
                fileData.getExepectedSize()
            );
        }
        else
        {
            FileCommands.createFat(
                extCmdFactory.create(),
                backingFile,
                fileData.getExepectedSize()
            );
        }
        String loDev = new String(
            LosetupCommands.attach(
                extCmdFactory.create(),
                backingFile
            )
            .stdoutData).trim();

        loDevs.put(loDev, backingFile.toString());
        fileData.setDevicePath(loDev);
    }

    @Override
    protected void resizeLvImpl(FileData vlmData)
        throws StorageException, AccessDeniedException
    {
        // no-op
    }

    @Override
    protected void deleteLvImpl(FileData fileData, String oldId)
        throws StorageException, SQLException
    {
        String devicePath = fileData.getDevicePath();
        LosetupCommands.detach(extCmdFactory.create(), devicePath);

        Path storageDirectory = fileData.getStorageDirectory();
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
                    FileCommands.delete(
                        storageDirectory,
                        newId
                    );
                    fileData.setExists(false);
                }
                catch (SQLException exc)
                {
                    throw new ImplementationError(exc);
                }
            }
        );
        loDevs.remove(devicePath);
    }

    @Override
    protected Map<String, Long> getFreeSpacesImpl() throws StorageException
    {
        Map<String, Long> freeSizes = FileUtils.getDirFreeSizes(changedStoragePoolStrings);
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
        List<FileData> fileDataList,
        List<SnapshotVolume> snapVlms
    )
        throws StorageException, AccessDeniedException
    {
        Map<String, FileInfo> infoList = FileUtils.getInfoList(extCmdFactory.create());
        for (Entry<String, FileInfo> entry : infoList.entrySet())
        {
            loDevs.put(entry.getValue().loPath.toString(), entry.getKey());
        }
        return infoList;
    }

    @Override
    protected String getDevicePath(String storageName, String lvId)
    {
        return null; // we cannot construct it from the given data. however, the devicePath is already
        // set on the corresponding FileData object
    }

    @Override
    protected String asLvIdentifier(ResourceName resourceName, String rscNameSuffix, VolumeNumber volumeNumber)
    {
        return String.format(
            FORMAT_RSC_TO_ID,
            resourceName.displayValue,
            rscNameSuffix,
            volumeNumber.value
        );
    }

    @Override
    protected String getStorageName(StorPool storPoolRef)
    {
        return getStorageDirectory(storPoolRef).toString();
    }

    protected Path getStorageDirectory(StorPool storPool)
    {
        Path dir;
        try
        {
            String dirStr = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getProps(storDriverAccCtx)
            )
                .getProp(StorageConstants.CONFIG_FILE_DIRECTORY_KEY);
            if (dirStr != null)
            {
                dir = Paths.get(dirStr);
            }
            else
            {
                dir = null;
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
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        Path directoryName = getStorageDirectory(storPool);
        if (directoryName == null)
        {
            throw new StorageException("Unset storage directory for " + storPool);
        }
        return directoryName.toFile().getTotalSpace();
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        Path directoryName = getStorageDirectory(storPool);
        if (directoryName == null)
        {
            throw new StorageException("Unset storage directory for " + storPool);
        }
        return directoryName.toFile().getFreeSpace();
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        Props props = DeviceLayerUtils.getNamespaceStorDriver(
            storPool.getProps(storDriverAccCtx)
        );
        StorageConfigReader.checkFileStorageDirectoryEntry(props);
    }

    @Override
    public void clearCache() throws StorageException
    {
        StringBuilder sb = new StringBuilder();
        for (Entry<String, String> entry : loDevs.entrySet())
        {
            sb.append(entry.getKey()).append(" ").append(entry.getValue()).append("\n");
        }
        if (sb.length() > 0)
        {
            sb.setLength(sb.length()-1);
        }

        File tmp = new File(LODEV_FILE_TMP);
        try (FileOutputStream fos = new FileOutputStream(tmp))
        {
            fos.write(sb.toString().getBytes());
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to write active loopback devices to " + LODEV_FILE_TMP, exc);
        }

        try
        {
            Files.move(tmp, new File(LODEV_FILE));
        }
        catch (IOException exc)
        {
            throw new StorageException("Failed to move " + LODEV_FILE_TMP + " to " + LODEV_FILE, exc);
        }

        super.clearCache();
    }

    @Override
    protected void setDevicePath(FileData vlmData, String devPath) throws SQLException
    {
        // ignored - devicePath is set when creating or when looking for fileData objects
    }

    @Override
    protected void setAllocatedSize(FileData vlmData, long size) throws SQLException
    {
        vlmData.setAllocatedSize(size);
    }

    @Override
    protected void setUsableSize(FileData vlmData, long size) throws SQLException
    {
        vlmData.setUsableSize(size);
    }

    @Override
    protected void setExpectedUsableSize(FileData vlmData, long size)
    {
        vlmData.setExepectedSize(size);
    }

    @Override
    protected String getStorageName(FileData vlmDataRef) throws SQLException
    {
        return vlmDataRef.getStorageDirectory().toString();
    }
}
