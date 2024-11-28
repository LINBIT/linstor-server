package com.linbit.linstor.layer.storage.file;

import com.linbit.PlatformStlt;
import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.core.devmgr.StltReadOnlyInfo.ReadOnlyVlmProviderInfo;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.storage.file.utils.FileCommands;
import com.linbit.linstor.layer.storage.file.utils.FileProviderUtils;
import com.linbit.linstor.layer.storage.file.utils.LosetupCommands;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class FileThinProvider extends FileProvider
{
    @Inject
    public FileThinProvider(AbsStorageProviderInit superInitRef, PlatformStlt platformStltRef)
    {
        super(
            superInitRef,
            "FILE THIN",
            DeviceProviderKind.FILE_THIN,
            platformStltRef
        );
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.FILE_THIN;
    }

    @Override
    protected void createLvImpl(FileData<Resource> fileData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Path backingFile = fileData.getStorageDirectory().resolve(fileData.getIdentifier());
        FileCommands.createThin(
            extCmdFactory.create(),
            backingFile,
            fileData.getExpectedSize()
        );
        createLoopDevice(fileData, backingFile);
    }

    @Override
    protected void resizeLvImpl(FileData<Resource> fileData)
        throws StorageException, AccessDeniedException
    {
        // no special command for resize, just "re-allocate" to the needed size
        FileCommands.createThin(
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
    protected long getAllocatedSizeFileImpl(ExtCmd extCmd, String pathRef) throws StorageException
    {
        return FileProviderUtils.getThinAllocatedSize(
            extCmd,
            pathRef
        );
    }

    @Override
    protected long getAllocatedSize(FileData<Resource> fileData) throws StorageException
    {
        return FileProviderUtils.getThinAllocatedSize(
            extCmdFactory.create(),
            getFullQualifiedIdentifier(fileData)
        );
    }

    @Override
    public Map<ReadOnlyVlmProviderInfo, Long> fetchAllocatedSizes(List<ReadOnlyVlmProviderInfo> vlmDataListRef)
        throws StorageException
    {
        Map<ReadOnlyVlmProviderInfo, Long> ret = new HashMap<>();
        for (ReadOnlyVlmProviderInfo roVlmProvInfo : vlmDataListRef)
        {
            @Nullable String devPath = roVlmProvInfo.getDevicePath();
            long allocatedSize;
            if (devPath == null)
            {
                allocatedSize = roVlmProvInfo.getOrigAllocatedSize();
            }
            else
            {
                allocatedSize = getAllocatedSizeFileImpl(extCmdFactory.create(), devPath);
            }
            ret.put(roVlmProvInfo, allocatedSize);
        }
        return ret;
    }
}
