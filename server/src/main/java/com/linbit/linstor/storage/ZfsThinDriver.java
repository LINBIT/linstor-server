package com.linbit.linstor.storage;

import java.io.File;
import java.util.Map;

import com.linbit.extproc.ExtCmd;
import com.linbit.fsevent.FileSystemWatch;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.storage.utils.Crypt;
import com.linbit.linstor.timer.CoreTimer;

public class ZfsThinDriver extends ZfsDriver
{
    public ZfsThinDriver(
        ErrorReporter errorReporter,
        FileSystemWatch fileSystemWatch,
        CoreTimer timer,
        StorageDriverKind storageDriverKind,
        StltConfigAccessor stltCfgAccessor,
        Crypt crypt
    )
    {
        super(errorReporter, fileSystemWatch, timer, storageDriverKind, stltCfgAccessor, crypt);
    }

    @Override
    public long getAllocated(String identifier)
        throws StorageException
    {
        final ExtCmd extCommand = new ExtCmd(timer, errorReporter);
        ZfsVolumeInfo volumeInfo = ZfsVolumeInfo.getInfo(extCommand, zfsCommand, pool, identifier);
        return volumeInfo.getUsed();
    }

    @Override
    protected String[] getCreateCommand(String identifier, long size)
    {
        return new String[]
        {
            zfsCommand,
            "create",
            "-s",
            "-V", size + "KB",
            pool + File.separator + identifier
        };
    }

    @Override
    protected String getPoolFromConfig(Map<String, String> config)
    {
        return getAsString(config, ApiConsts.KEY_STOR_POOL_ZPOOLTHIN, pool);
    }
}
