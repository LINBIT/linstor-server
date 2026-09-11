package com.linbit.linstor.layer;

import com.linbit.ImplementationError;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.test.factories.ResourceTestFactory;
import com.linbit.linstor.test.factories.StorPoolTestFactory;
import com.linbit.linstor.test.factories.VolumeTestFactory;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import jakarta.inject.Inject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LayerSizeCalculatorTest extends GenericDbBase
{
    @Inject LayerSizeHelper layerSizeHelper;

    @Inject ResourceTestFactory rscFactory;
    @Inject VolumeTestFactory vlmFactory;
    @Inject StorPoolTestFactory spFactory;

    @Inject EncryptionHelper encrHelper;

    @Before
    public void setup() throws Exception
    {
        setUpAndEnterScope();
        byte[] testMasterKey = encrHelper.generateSecret();
        encrHelper.setPassphraseImpl("testPassphrase".getBytes(), testMasterKey);
        @Nullable ReadOnlyProps encryptedNamespace = encrHelper.getEncryptedNamespace();
        if (encryptedNamespace == null)
        {
            throw new ImplementationError(
                "encryption namespace must not be null after encrHelper.setPassphraseImpl was called"
            );
        }
        encrHelper.setCryptKey(testMasterKey, encryptedNamespace, false);
    }

    @Test
    public void genericSizeTest() throws Exception
    {
        Resource rsc = new Builder().build();
        // assertVlmSize(rsc, DeviceLayerKind.DRBD, 12 * 1024, 12 * 1024);
        // STORAGE layer does not override allocated size to not screw up satellite's tracking of that field
        assertVlmSize(rsc, DeviceLayerKind.STORAGE, 12 * 1024, -1);
    }

    private void assertVlmSize(
        Resource rscRef,
        DeviceLayerKind layerRef,
        long expUsableSizeRef,
        long expAllocatedSizeRef
    )
        throws ValueOutOfRangeException
    {
        assertVlmSize(rscRef, 0, "", layerRef, expUsableSizeRef, expAllocatedSizeRef);
    }

    private void assertVlmSize(
        Resource rscRef,
        int vlmNrRef,
        String rscSuffixRef,
        DeviceLayerKind layerRef,
        long expUsableSizeRef,
        long expAllocatedSizeRef
    )
        throws ValueOutOfRangeException
    {
        Set<AbsRscLayerObject<Resource>> rscDataByLayerSet = LayerRscUtils.getRscDataByLayer(
            rscRef.getLayerData(),
            layerRef
        );
        @Nullable AbsRscLayerObject<Resource> rscData = null;
        for (AbsRscLayerObject<Resource> elem : rscDataByLayerSet)
        {
            if (elem.getResourceNameSuffix().equals(rscSuffixRef))
            {
                rscData = elem;
                break;
            }
        }
        assertNotNull(rscData);
        @Nullable VlmProviderObject<Resource> vlmData = rscData.getVlmProviderObject(new VolumeNumber(vlmNrRef));
        assertNotNull(vlmData);
        assertEquals(expAllocatedSizeRef, vlmData.getAllocatedSize());
        assertEquals(expUsableSizeRef, vlmData.getUsableSize());
    }

    private class Builder
    {
        String nodeName = "node";
        String rscName = "rsc";
        int vlmNr = 0;
        String spName = "mySp";
        DeviceProviderKind spKind = DeviceProviderKind.LVM;

        long size = 10 * 1024; // 10 MiB
        List<DeviceLayerKind> layerStack = Arrays.asList(
            DeviceLayerKind.DRBD,
            DeviceLayerKind.STORAGE
        );

        LayerPayload layerPayload = new LayerPayload();
        private Map<String, String> spPropsMap = new HashMap<>();

        private Resource build() throws Exception
        {
            Resource rsc = rscFactory.builder(nodeName, rscName)
                .setLayerStack(layerStack)
                .build();
            StorPool storPool = spFactory.builder(nodeName, spName)
                .setDriverKind(spKind)
                .build();
            storPool.getProps().map().putAll(spPropsMap);
            layerPayload.putStorageVlmPayload("", vlmNr, storPool);
            vlmFactory.builder(nodeName, rscName, vlmNr)
                .setSize(size)
                .setLayerPayload(layerPayload)
                .build();

            return rsc;
        }

        public Builder withSize(long sizeRef)
        {
            size = sizeRef;
            return this;
        }

        public Builder withLayerPayload(LayerPayload layerPayloadRef)
        {
            layerPayload = layerPayloadRef;
            return this;
        }

        public Builder withSpProp(String fullKey, String value)
        {
            spPropsMap.put(fullKey, value);
            return this;
        }
    }
}
