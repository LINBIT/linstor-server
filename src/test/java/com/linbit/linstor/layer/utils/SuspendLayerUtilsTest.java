package com.linbit.linstor.layer.utils;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.Arrays;
import java.util.LinkedHashSet;

import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SuspendLayerUtilsTest
{
    private static AbsRscLayerObject layer(DeviceLayerKind kind, AbsRscLayerObject... children)
    {
        AbsRscLayerObject obj = mock(AbsRscLayerObject.class);
        when(obj.getLayerKind()).thenReturn(kind);
        when(obj.getChildren()).thenReturn(new LinkedHashSet<>(Arrays.asList(children)));
        return obj;
    }

    private static AbsResource rsc(AbsRscLayerObject root)
    {
        AbsResource rsc = mock(AbsResource.class);
        when(rsc.getLayerData()).thenReturn(root);
        return rsc;
    }

    @Test
    public void suspendDrbdLuksStorageSuspendsOnlyDrbd() throws DatabaseException
    {
        AbsRscLayerObject storage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject luks = layer(DeviceLayerKind.LUKS, storage);
        AbsRscLayerObject drbd = layer(DeviceLayerKind.DRBD, luks);

        SuspendLayerUtils.suspendIo(rsc(drbd));

        verify(drbd, times(1)).setShouldSuspendIo(true);
        verify(luks, never()).setShouldSuspendIo(anyBoolean());
        verify(storage, never()).setShouldSuspendIo(anyBoolean());
    }

    @Test
    public void suspendLuksAsTopmostLayerSuspendsLuks() throws DatabaseException
    {
        AbsRscLayerObject storage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject luks = layer(DeviceLayerKind.LUKS, storage);

        SuspendLayerUtils.suspendIo(rsc(luks));

        verify(luks, times(1)).setShouldSuspendIo(true);
        verify(storage, never()).setShouldSuspendIo(anyBoolean());
    }

    @Test
    public void suspendDrbdWritecacheStorageSuspendsDrbdAndWritecache() throws DatabaseException
    {
        AbsRscLayerObject dataStorage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject cacheStorage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject writecache = layer(DeviceLayerKind.WRITECACHE, dataStorage, cacheStorage);
        AbsRscLayerObject drbd = layer(DeviceLayerKind.DRBD, writecache);

        SuspendLayerUtils.suspendIo(rsc(drbd));

        verify(drbd, times(1)).setShouldSuspendIo(true);
        verify(writecache, times(1)).setShouldSuspendIo(true);
        verify(dataStorage, never()).setShouldSuspendIo(anyBoolean());
        verify(cacheStorage, never()).setShouldSuspendIo(anyBoolean());
    }

    @Test
    public void suspendDrbdCacheStorageSuspendsDrbdAndCache() throws DatabaseException
    {
        AbsRscLayerObject storage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject cache = layer(DeviceLayerKind.CACHE, storage);
        AbsRscLayerObject drbd = layer(DeviceLayerKind.DRBD, cache);

        SuspendLayerUtils.suspendIo(rsc(drbd));

        verify(drbd, times(1)).setShouldSuspendIo(true);
        verify(cache, times(1)).setShouldSuspendIo(true);
        verify(storage, never()).setShouldSuspendIo(anyBoolean());
    }

    @Test
    public void suspendKeepsDescendingThroughUnlistedLayers() throws DatabaseException
    {
        AbsRscLayerObject dataStorage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject writecacheCacheStorage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject writecache = layer(DeviceLayerKind.WRITECACHE, dataStorage, writecacheCacheStorage);
        AbsRscLayerObject bcacheCacheStorage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject bcache = layer(DeviceLayerKind.BCACHE, writecache, bcacheCacheStorage);
        AbsRscLayerObject drbd = layer(DeviceLayerKind.DRBD, bcache);

        SuspendLayerUtils.suspendIo(rsc(drbd));

        verify(drbd, times(1)).setShouldSuspendIo(true);
        verify(bcache, never()).setShouldSuspendIo(anyBoolean());
        verify(bcacheCacheStorage, never()).setShouldSuspendIo(anyBoolean());
        verify(writecache, times(1)).setShouldSuspendIo(true);
        verify(dataStorage, never()).setShouldSuspendIo(anyBoolean());
        verify(writecacheCacheStorage, never()).setShouldSuspendIo(anyBoolean());
    }

    @Test
    public void suspendDrbdNvmeLuksStorageSuspendsOnlyDrbd() throws DatabaseException
    {
        AbsRscLayerObject storage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject luks = layer(DeviceLayerKind.LUKS, storage);
        AbsRscLayerObject nvme = layer(DeviceLayerKind.NVME, luks);
        AbsRscLayerObject drbd = layer(DeviceLayerKind.DRBD, nvme);

        SuspendLayerUtils.suspendIo(rsc(drbd));

        verify(drbd, times(1)).setShouldSuspendIo(true);
        verify(nvme, never()).setShouldSuspendIo(anyBoolean());
        verify(luks, never()).setShouldSuspendIo(anyBoolean());
        verify(storage, never()).setShouldSuspendIo(anyBoolean());
    }

    @Test
    public void resumeClearsEveryLayer() throws DatabaseException
    {
        AbsRscLayerObject storage = layer(DeviceLayerKind.STORAGE);
        AbsRscLayerObject luks = layer(DeviceLayerKind.LUKS, storage);
        AbsRscLayerObject drbd = layer(DeviceLayerKind.DRBD, luks);

        SuspendLayerUtils.resumeIo(rsc(drbd));

        verify(drbd, times(1)).setShouldSuspendIo(false);
        verify(luks, times(1)).setShouldSuspendIo(false);
        verify(storage, times(1)).setShouldSuspendIo(false);
    }
}
