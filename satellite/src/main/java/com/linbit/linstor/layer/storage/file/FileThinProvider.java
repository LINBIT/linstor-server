package com.linbit.linstor.layer.storage.file;

import com.linbit.extproc.ExtCmd;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.layer.storage.WipeHandler;
import com.linbit.linstor.layer.storage.file.utils.FileCommands;
import com.linbit.linstor.layer.storage.file.utils.FileUtils;
import com.linbit.linstor.layer.storage.file.utils.LosetupCommands;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.file.FileData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.nio.file.Path;

@Singleton
public class FileThinProvider extends FileProvider
{
    @Inject
    public FileThinProvider(
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
            "FILE THIN",
            DeviceProviderKind.FILE_THIN
        );
    }

    @Override
    protected void createLvImpl(FileData<Resource> fileData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        Path backingFile = fileData.getStorageDirectory().resolve(fileData.getIdentifier());
        FileCommands.createThin(
            extCmdFactory.create(),
            backingFile,
            fileData.getExepectedSize()
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
            fileData.getExepectedSize()
        );
        LosetupCommands.resize(
            extCmdFactory.create(),
            fileData.getDevicePath()
        );
    }

    @Override
    protected long getAllocatedSizeFileImpl(ExtCmd extCmd, String pathRef) throws StorageException
    {
        return FileUtils.getThinAllocatedSize(
            extCmd,
            pathRef
        );
    }

    @Override
    protected long getAllocatedSize(FileData<Resource> fileData) throws StorageException
    {
        return FileUtils.getThinAllocatedSize(
            extCmdFactory.create(),
            getFullQualifiedIdentifier(fileData)
        );
    }

}
