package com.linbit.linstor.layer.luks;

import com.linbit.ImplementationError;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.apicallhandler.controller.helpers.EncryptionHelper;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.layer.LayerPayload;
import com.linbit.linstor.layer.LayerSizeHelper;
import com.linbit.linstor.layer.storage.StorageLayerSizeCalculator;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.StorageConstants;
import com.linbit.linstor.storage.data.adapter.luks.LuksRscData;
import com.linbit.linstor.storage.data.adapter.luks.LuksVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.kinds.ExtTools;
import com.linbit.linstor.storage.kinds.ExtToolsInfo;
import com.linbit.linstor.test.factories.ResourceTestFactory;
import com.linbit.linstor.test.factories.StorPoolTestFactory;
import com.linbit.linstor.test.factories.VolumeDefinitionTestFactory.VolumeDefinitionBuilder;
import com.linbit.linstor.test.factories.VolumeTestFactory;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;
import com.linbit.linstor.utils.layer.LayerRscUtils;

import jakarta.inject.Inject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests the {@link LuksLayerSizeCalculator}. However this is not done by fully mocking the STORAGE layer just
 * by not properly configuring it, i.e. ignoring allocationGranularity. So we do not round up to LVM or ZFS
 * defaults, but let the STORAGE layer use allocation granularity of 1. This way the STORAGE part of the
 * size calculation basically becomes a noop.
 *
 * The other way to implement this test would be to fully mock the {@link StorageLayerSizeCalculator}.
 */
public class LuksLayerSizeCalculatorTest extends GenericDbBase
{
    private static final String PROP_KEY_LUKS_FORMAT = ApiConsts.NAMESPC_STORAGE_DRIVER + "/" +
        ApiConsts.KEY_STOR_DRIVER_LUKS_FORMAT_OPTIONS;
    private static final String PROP_KEY_OPT_IO_SIZE = StorageConstants.NAMESPACE_INTERNAL + '/' +
        StorageConstants.BLK_DEV_OPT_IO_SIZE;

    @Inject LuksLayerSizeCalculator calc;

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
    public void noCryptsetupTest() throws Exception
    {
        Builder builder = new Builder().withCryptSetupVersion(null, null, null);
        try
        {
            builder.build();
            fail("Exception expected without supported cryptsetup");
        }
        catch (ApiRcException expected)
        {
            // expected
        }
    }

    @Test
    public void luks1Test() throws Exception
    {
        Builder builder = new Builder().withCryptSetupVersion(2, 0, 0);
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 2 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2Test() throws Exception
    {
        Builder builder = new Builder();
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 16 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithOffsetTest() throws Exception
    {
        Builder builder = new Builder().withSpProp(PROP_KEY_LUKS_FORMAT, "--offset 65536"); // 32M offset
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 32 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithLargeOptimalIoSizeTest() throws Exception
    {
        Builder builder = new Builder().withSpProp(PROP_KEY_OPT_IO_SIZE, Long.toString(32L << 20));// 32M
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 32 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithSmallOptimalIoSizeTest() throws Exception
    {
        Builder builder = new Builder().withSpProp(PROP_KEY_OPT_IO_SIZE, Long.toString(4L << 10));// 4k
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 16 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithOptimalIoSizeAndOffsetTest() throws Exception
    {
        Builder builder = new Builder().withSpProp(PROP_KEY_OPT_IO_SIZE, Long.toString(32L << 20))// 32M
            .withSpProp(
                PROP_KEY_LUKS_FORMAT,
                LuksLayerSizeCalculator.LUKS2_OPT_OFFSET + " 32768" // overrules optimal_io_size
            );
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 16 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithKeyslotsSizeTest() throws Exception
    {
        Builder builder = new Builder().withSpProp(
            PROP_KEY_LUKS_FORMAT,
            LuksLayerSizeCalculator.LUKS2_OPT_KEYSLOTS_SIZE + " 128M"
        );
        LuksVlmData<Resource> vlmData = builder.build();
        // expected: 129MB whereas 128MB come from keySlots + <1MB from luks2-metadata, which gets rounded up within
        // cryptsetup to the next MB
        assertEquals(builder.size + 129 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithMetadataSizeTest() throws Exception
    {
        Builder builder = new Builder().withSpProp(
            PROP_KEY_LUKS_FORMAT,
            LuksLayerSizeCalculator.LUKS2_OPT_METADATA_SIZE + " 4M"
        );
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 16 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithKeyslotsAndMetadataSizeTest() throws Exception
    {
        // as an additional test, this test uses the "key=value" syntax instead of "key value".
        Builder builder = new Builder().withSpProp(
            PROP_KEY_LUKS_FORMAT,
            LuksLayerSizeCalculator.LUKS2_OPT_METADATA_SIZE + "=4M " +
                LuksLayerSizeCalculator.LUKS2_OPT_KEYSLOTS_SIZE + "=128M"
        );
        LuksVlmData<Resource> vlmData = builder.build();
        // 128M + 2*4M == 136M expected
        assertEquals(builder.size + 136 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2WithOffsetAndKeyslotsSizeTest() throws Exception
    {
        Builder builder = new Builder().withSpProp(
            PROP_KEY_LUKS_FORMAT,
            LuksLayerSizeCalculator.LUKS2_OPT_KEYSLOTS_SIZE + " 128M " +
                LuksLayerSizeCalculator.LUKS2_OPT_OFFSET + " 65536" // offset overrules keyslots-size
        );
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size + 32 * 1024, vlmData.getAllocatedSize());
        assertEquals(builder.size, vlmData.getUsableSize());
    }

    @Test
    public void luks2SimpleGrossSizeTest() throws Exception
    {
        Builder builder = new Builder()
            .withSize(100 * 1024) // 100Mib
            .withGrossSize(true);
        LuksVlmData<Resource> vlmData = builder.build();
        assertEquals(builder.size, vlmData.getAllocatedSize());
        assertEquals(builder.size - 16 * 1024, vlmData.getUsableSize());
    }

    private class Builder
    {
        private String nodeName = "node";
        private String rscName = "rsc";
        private int vlmNr = 0;
        private String spName = "mySp";
        private DeviceProviderKind spKind = DeviceProviderKind.ZFS;

        private long size = 10 * 1024; // 10 MiB
        private List<DeviceLayerKind> layerStack = Arrays.asList(
            DeviceLayerKind.LUKS,
            DeviceLayerKind.STORAGE
        );

        private LayerPayload layerPayload = new LayerPayload();
        private Map<String, String> spPropsMap = new HashMap<>();

        private ExtToolsInfo.Version cryptSetupVersion = new ExtToolsInfo.Version(2, 4, 3);
        private boolean grossSize;

        private LuksVlmData<Resource> build() throws Exception
        {
            Node node = nodeTestFactory.builder(nodeName).build();
            Peer mockedPeer = Mockito.mock(Peer.class);
            ExtToolsManager mockedExtToolsMgr = Mockito.mock(ExtToolsManager.class);
            Mockito.when(mockedPeer.getExtToolsManager()).thenReturn(mockedExtToolsMgr);
            Mockito.when(mockedExtToolsMgr.getExtToolInfo(ExtTools.CRYPT_SETUP))
                .thenReturn(
                    new ExtToolsInfo(ExtTools.CRYPT_SETUP, cryptSetupVersion != null, cryptSetupVersion, null)
                );
            node.setPeer(mockedPeer);

            Resource rsc = rscFactory.builder(nodeName, rscName)
                .setLayerStack(layerStack)
                .build();
            StorPool storPool = spFactory.builder(nodeName, spName)
                .setDriverKind(spKind)
                .build();
            storPool.getProps().map().putAll(spPropsMap);
            layerPayload.putStorageVlmPayload("", vlmNr, storPool);
            VolumeDefinitionBuilder vlmDfnBuilder = volumeDefinitionTestFactory.builder(rscName)
                .setSize(size);
            if (grossSize)
            {
                vlmDfnBuilder
                    .setFlags(new VolumeDefinition.Flags[]
                    { VolumeDefinition.Flags.GROSS_SIZE });
            }
            vlmDfnBuilder.build();
            vlmFactory.builder(nodeName, rscName, vlmNr)
                .setLayerPayload(layerPayload)
                .build();

            Set<AbsRscLayerObject<Resource>> rscDataSet = LayerRscUtils.getRscDataByLayer(
                rsc.getLayerData(),
                DeviceLayerKind.LUKS
            );
            LuksRscData<Resource> luksRscData = (LuksRscData<Resource>) rscDataSet.iterator().next();
            return luksRscData.getVlmProviderObject(new VolumeNumber(vlmNr));
        }

        public Builder withGrossSize(boolean grossSizeRef)
        {
            grossSize = grossSizeRef;
            return this;
        }

        public Builder withSize(long sizeRef)
        {
            size = sizeRef;
            return this;
        }

        public Builder withSpProp(String fullKey, String value)
        {
            spPropsMap.put(fullKey, value);
            return this;
        }

        public Builder withCryptSetupVersion(
            @Nullable Integer majorRef,
            @Nullable Integer minorRef,
            @Nullable Integer patchRef
        )
        {
            if (majorRef == null)
            {
                cryptSetupVersion = null;
            }
            else
            {
                cryptSetupVersion = new ExtToolsInfo.Version(majorRef, minorRef, patchRef);
            }
            return this;
        }
    }
}
