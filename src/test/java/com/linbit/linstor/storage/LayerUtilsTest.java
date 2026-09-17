package com.linbit.linstor.storage;

import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.storage.data.adapter.nvme.NvmeRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.utils.LayerUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.DRBD;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.LUKS;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.NVME;
import static com.linbit.linstor.storage.kinds.DeviceLayerKind.STORAGE;

public class LayerUtilsTest
{
    @Test
    public void test()
    {
        assertTrue(check(STORAGE));
        assertTrue(check(DRBD, STORAGE));
        assertTrue(check(LUKS, STORAGE));
        assertTrue(check(DRBD, LUKS, STORAGE));

        assertFalse(check());
        assertFalse(check(DRBD));
        assertFalse(check(LUKS));
        assertFalse(check(DRBD, DRBD));
        assertFalse(check(DRBD, LUKS));
        assertFalse(check(LUKS, DRBD));
        assertFalse(check(LUKS, LUKS));
        assertFalse(check(STORAGE, DRBD));
        assertFalse(check(STORAGE, LUKS));
        assertFalse(check(STORAGE, STORAGE));
        assertFalse(check(DRBD, STORAGE, LUKS));
        assertFalse(check(STORAGE, DRBD, LUKS));
        assertFalse(check(STORAGE, LUKS, DRBD));
        assertFalse(check(LUKS, STORAGE, DRBD));
    }

    private boolean check(DeviceLayerKind... kinds)
    {
        return LayerUtils.isLayerKindStackAllowed(Arrays.asList(kinds));
    }

    /*
     * getUsedDeviceLayerKinds: which layers a satellite has to support for a given resource. Layers that a satellite
     * never executes for this resource (above an NVMe or EBS target, below an NVMe initiator) must not be required.
     */

    @Test
    public void usedLayersPlainDrbdOverStorage() throws Exception
    {
        AbsRscLayerObject<Resource> root = layer(DRBD, storage(DeviceProviderKind.LVM));

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(DRBD, STORAGE);
    }

    @Test
    public void usedLayersEbsTargetDropsLayersAbove() throws Exception
    {
        // the EBS target node is a special satellite inside the controller process; it must not need DRBD
        AbsRscLayerObject<Resource> root = layer(DRBD, storage(DeviceProviderKind.EBS_TARGET));

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(STORAGE);
    }

    @Test
    public void usedLayersEbsInitiatorKeepsLayersAbove() throws Exception
    {
        // the initiator attaches the volume and runs DRBD on it
        AbsRscLayerObject<Resource> root = layer(DRBD, storage(DeviceProviderKind.EBS_INIT));

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(DRBD, STORAGE);
    }

    @Test
    public void usedLayersMixedEbsTargetAndOtherVolumesKeepsLayersAbove() throws Exception
    {
        AbsRscLayerObject<Resource> root = layer(
            DRBD,
            storage(DeviceProviderKind.EBS_TARGET, DeviceProviderKind.LVM)
        );

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(DRBD, STORAGE);
    }

    @Test
    public void usedLayersStorageWithoutVolumesKeepsLayersAbove() throws Exception
    {
        // a resource created before its first volume definition (the CSI order) must still require DRBD
        AbsRscLayerObject<Resource> root = layer(DRBD, storage());

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(DRBD, STORAGE);
    }

    @Test
    public void usedLayersNvmeInitiatorDropsLayersBelow() throws Exception
    {
        AbsRscLayerObject<Resource> root = layer(DRBD, nvme(true, storage(DeviceProviderKind.LVM)));

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(DRBD, NVME);
    }

    @Test
    public void usedLayersNvmeTargetDropsLayersAbove() throws Exception
    {
        AbsRscLayerObject<Resource> root = layer(DRBD, nvme(false, storage(DeviceProviderKind.LVM)));

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(NVME, STORAGE);
    }

    @Test
    public void usedLayersLuksBetweenDrbdAndEbsTargetIsDroppedAsWell() throws Exception
    {
        AbsRscLayerObject<Resource> root = layer(DRBD, layer(LUKS, storage(DeviceProviderKind.EBS_TARGET)));

        assertThat(LayerUtils.getUsedDeviceLayerKinds(root)).containsExactly(STORAGE);
    }

    @SuppressWarnings("unchecked")
    private static AbsRscLayerObject<Resource> layer(DeviceLayerKind kind, AbsRscLayerObject<Resource> child)
    {
        AbsRscLayerObject<Resource> mock = Mockito.mock(AbsRscLayerObject.class);
        Mockito.when(mock.getLayerKind()).thenReturn(kind);
        Mockito.when(mock.getChildBySuffix("")).thenReturn(child);
        Mockito.doReturn(new HashMap<>()).when(mock).getVlmLayerObjects();
        return mock;
    }

    @SuppressWarnings("unchecked")
    private static AbsRscLayerObject<Resource> nvme(boolean initiator, AbsRscLayerObject<Resource> child)
    {
        NvmeRscData<Resource> mock = Mockito.mock(NvmeRscData.class);
        Mockito.when(mock.getLayerKind()).thenReturn(NVME);
        Mockito.when(mock.isInitiator()).thenReturn(initiator);
        Mockito.when(mock.getChildBySuffix("")).thenReturn(child);
        Mockito.doReturn(new HashMap<>()).when(mock).getVlmLayerObjects();
        return mock;
    }

    @SuppressWarnings("unchecked")
    private static AbsRscLayerObject<Resource> storage(DeviceProviderKind... providerKinds) throws Exception
    {
        Map<VolumeNumber, VlmProviderObject<Resource>> vlms = new HashMap<>();
        for (int vlmNr = 0; vlmNr < providerKinds.length; vlmNr++)
        {
            VlmProviderObject<Resource> vlm = Mockito.mock(VlmProviderObject.class);
            Mockito.when(vlm.getProviderKind()).thenReturn(providerKinds[vlmNr]);
            vlms.put(new VolumeNumber(vlmNr), vlm);
        }
        AbsRscLayerObject<Resource> mock = Mockito.mock(AbsRscLayerObject.class);
        Mockito.when(mock.getLayerKind()).thenReturn(STORAGE);
        Mockito.when(mock.getChildBySuffix("")).thenReturn(null);
        Mockito.doReturn(vlms).when(mock).getVlmLayerObjects();
        return mock;
    }
}
