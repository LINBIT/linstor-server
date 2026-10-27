package com.linbit.linstor.layer.drbd;

import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.data.provider.zfs.ZfsData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DrbdLayerAdoptedDeviceTest
{
    private static AbsStorageVlmData<?> storageVlm(boolean createdByStlt)
    {
        AbsStorageVlmData<?> vlmData = Mockito.mock(AbsStorageVlmData.class);
        when(vlmData.isCreatedByStlt()).thenReturn(createdByStlt);
        return vlmData;
    }

    private static List<VlmProviderObject<?>> devices(VlmProviderObject<?>... devicesRef)
    {
        return Arrays.asList(devicesRef);
    }

    @Test
    public void createdDevice()
    {
        assertThat(DrbdLayer.isCreatedByStlt(devices(storageVlm(true)))).isTrue();
    }

    @Test
    public void adoptedDevice()
    {
        // e.g. a zvol left behind by a failed delete, found under the expected name
        assertThat(DrbdLayer.isCreatedByStlt(devices(storageVlm(false)))).isFalse();
    }

    @Test
    public void oneAdoptedDeviceOfSeveral()
    {
        assertThat(DrbdLayer.isCreatedByStlt(devices(storageVlm(true), storageVlm(false)))).isFalse();
    }

    @Test
    public void untrackedProviderCountsAsCreated()
    {
        VlmProviderObject<?> other = Mockito.mock(VlmProviderObject.class);
        assertThat(DrbdLayer.isCreatedByStlt(devices(other))).isTrue();
        assertThat(DrbdLayer.isCreatedByStlt(Collections.emptyList())).isTrue();
    }

    @Test
    public void volBlockSizeMismatch()
    {
        // requested in bytes, actual in KiB as reported by "zfs list"
        assertThat(DrbdLayer.isVolBlockSizeMismatch(16_384L, 8L)).isTrue();
        assertThat(DrbdLayer.isVolBlockSizeMismatch(16_384L, 16L)).isFalse();
        assertThat(DrbdLayer.isVolBlockSizeMismatch(null, 8L)).isFalse();
        assertThat(DrbdLayer.isVolBlockSizeMismatch(16_384L, null)).isFalse();
    }

    private static ZfsData<?> zfsVlm(boolean refused, Long requestedBytes, Long actualKiB)
    {
        ZfsData<?> zfsData = Mockito.mock(ZfsData.class);
        when(zfsData.isAdoptionRefused()).thenReturn(refused);
        when(zfsData.getRequestedVolBlockSize()).thenReturn(requestedBytes);
        when(zfsData.getExtentSize()).thenReturn(actualKiB);
        return zfsData;
    }

    @Test
    public void refusedAndStillMismatchingStaysRefused()
    {
        ZfsData<?> zfsData = zfsVlm(true, 16_384L, 8L);
        assertThat(DrbdLayer.refreshAdoptionRefusals(devices(zfsData), false)).containsExactly(zfsData);
        verify(zfsData, never()).setAdoptionRefused(false);
    }

    @Test
    public void refusedButFixedIsCleared()
    {
        // e.g. ZfscreateOptions changed to match the dataset
        ZfsData<?> zfsData = zfsVlm(true, 8_192L, 8L);
        assertThat(DrbdLayer.refreshAdoptionRefusals(devices(zfsData), false)).isEmpty();
        verify(zfsData).setAdoptionRefused(false);
    }

    @Test
    public void notRefusedIsIgnored()
    {
        ZfsData<?> zfsData = zfsVlm(false, 16_384L, 8L);
        assertThat(DrbdLayer.refreshAdoptionRefusals(devices(zfsData), false)).isEmpty();
        verify(zfsData, never()).setAdoptionRefused(false);
    }

    @Test
    public void adoptedMismatchIsRefusedQuietlyWhileDeleting()
    {
        // e.g. the rd is being deleted and a satellite restart lost the marker
        ZfsData<?> zfsData = zfsVlm(false, 16_384L, 8L);
        when(zfsData.isCreatedByStlt()).thenReturn(false);
        assertThat(DrbdLayer.refreshAdoptionRefusals(devices(zfsData), true)).containsExactly(zfsData);
        verify(zfsData).setAdoptionRefused(true);
    }

    @Test
    public void createdDeviceIsNeverRefusedWhileDeleting()
    {
        ZfsData<?> zfsData = zfsVlm(false, 16_384L, 8L);
        when(zfsData.isCreatedByStlt()).thenReturn(true);
        assertThat(DrbdLayer.refreshAdoptionRefusals(devices(zfsData), true)).isEmpty();
        verify(zfsData, never()).setAdoptionRefused(true);
    }
}
