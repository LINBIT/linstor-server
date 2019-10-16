package com.linbit.linstor.storage.layer.provider.zfs;

import com.linbit.ImplementationError;
import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.annotation.DeviceManagerContext;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.DeviceLayer.NotificationListener;
import com.linbit.linstor.storage.layer.provider.WipeHandler;
import com.linbit.linstor.storage.utils.DeviceLayerUtils;
import com.linbit.linstor.storage.utils.ZfsCommands;
import com.linbit.linstor.storage.utils.ZfsUtils;
import com.linbit.linstor.storage.utils.ZfsUtils.ZfsInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import java.util.Collections;
import java.util.HashMap;

@Singleton
public class ZfsThinProvider extends ZfsProvider
{
    @Inject
    public ZfsThinProvider(
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
            "ZFS-Thin",
            DeviceProviderKind.ZFS_THIN
        );
    }

    @Override
    protected void createLvImpl(ZfsData<Resource> vlmData)
        throws StorageException, AccessDeniedException, DatabaseException
    {
        ZfsCommands.create(
            extCmdFactory.create(),
            vlmData.getZPool(),
            asLvIdentifier(vlmData),
            vlmData.getExepectedSize(),
            true
        );
    }

    @Override
    protected String getZPool(StorPool storPool) throws AccessDeniedException
    {
        String zPool;
        try
        {
            zPool = DeviceLayerUtils.getNamespaceStorDriver(
                storPool.getProps(storDriverAccCtx)
            ).getProp(StorageConstants.CONFIG_ZFS_THIN_POOL_KEY);
        }
        catch (InvalidKeyException exc)
        {
            throw new ImplementationError(exc);
        }
        return zPool;
    }

    @Override
    public void checkConfig(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String thinZpoolName = getZPool(storPool);
        if (thinZpoolName == null)
        {
            throw new StorageException(
                "zPool name not given for storPool '" +
                    storPool.getName().displayValue + "'"
            );
        }
        thinZpoolName = thinZpoolName.trim();
        HashMap<String, ZfsInfo> zfsList = ZfsUtils.getThinZPoolsList(extCmdFactory.create());
        if (!zfsList.containsKey(thinZpoolName))
        {
            throw new StorageException("no zfs dataset found with name '" + thinZpoolName + "'");
        }
    }

    @Override
    public long getPoolCapacity(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String zPoolName = getZpoolOnlyName(storPool);

        // do not use the thin version, we have to ask the actual zpool, not the thin "pool"
        return ZfsUtils.getZPoolTotalSize(
            extCmdFactory.create(),
            Collections.singleton(zPoolName)
        ).get(zPoolName);
    }

    @Override
    public long getPoolFreeSpace(StorPool storPool) throws StorageException, AccessDeniedException
    {
        String thinZpoolName = getZPool(storPool);
        if (thinZpoolName == null)
        {
            throw new StorageException("Unset thin zfs dataset for " + storPool);
        }

        return ZfsUtils.getThinZPoolsList(
            extCmdFactory.create()
        ).get(thinZpoolName).usableSize;
    }
}
