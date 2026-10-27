package com.linbit.linstor.layer.drbd;

import com.linbit.linstor.storage.data.provider.AbsStorageVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
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
}
