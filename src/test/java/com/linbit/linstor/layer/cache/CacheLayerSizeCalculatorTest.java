package com.linbit.linstor.layer.cache;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.layer.LayerSizeHelper;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.data.RscLayerSuffixes;
import com.linbit.linstor.storage.data.adapter.cache.CacheRscData;
import com.linbit.linstor.storage.data.adapter.cache.CacheVlmData;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import javax.inject.Inject;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CacheLayerSizeCalculatorTest extends GenericDbBase
{
    private static final int LVM_DFLT_EXTENT_SIZE_IN_BYTES = 4 << 20;

    @Inject LayerSizeHelper layerSizeHelper;

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
    }

    // @SuppressFBWarnings("UM_UNNECESSARY_MATH")
    @Test
    public void sizeCalcTest() throws Exception
    {
        // basic formula for the size of the metadata device: 4MB + 16B * <nr_of_blocks>

        new SizeTest(1 << 30).expect(calcMetaDataSize((1L << 40) * 0.05, 4096, LVM_DFLT_EXTENT_SIZE_IN_BYTES));
        new SizeTest(1 << 30).withRscSuffixToTest(RscLayerSuffixes.SUFFIX_CACHE_CACHE)
            .expect(52432 << 10); // technically 53_687_092, but that is 51.200000763 GB, or
                                  // 52428.80078125 MB, which gets rounded up by LVM to 52432 MB
        new SizeTest(1 << 30).withCacheSize("1%").expect(calcMetaDataSize((1L << 40) * 0.01));
        new SizeTest(1 << 30).withBlockSize("512")
            .expect(calcMetaDataSize((1L << 40) * 0.05, 512, LVM_DFLT_EXTENT_SIZE_IN_BYTES));
        new SizeTest(1 << 30).withBlockSize("512")
            .withCacheSize("10%")
            .expect(calcMetaDataSize((1L << 40) * 0.1, 512, LVM_DFLT_EXTENT_SIZE_IN_BYTES));
    }

    private static long calcMetaDataSize(double cacheSize)
    {
        return calcMetaDataSize(
            cacheSize,
            CacheLayerSizeCalculator.DFLT_BLOCK_SIZE_IN_BYTES,
            LVM_DFLT_EXTENT_SIZE_IN_BYTES
        );
    }

    private static long calcMetaDataSize(double cacheSizeInBytes, long dfltBlockSizeInBytesRef, int extentSizeInBytes)
    {
        return Math.round(
            Math.ceil(
                ((cacheSizeInBytes / dfltBlockSizeInBytesRef) // number of blocks of the cache device
                    * 16 + (4 << 20) // numberOfBlocks * 16B + 4MB
                ) / extentSizeInBytes // so we can round up (using ceil + round) to the next extent
            )
        ) * extentSizeInBytes // go back to bytes based
            / 1024; // but linstor wants to calculate in kib
    }

    private class SizeTest
    {
        private String nodeName = "node";
        private String rscName = "rsc";
        private String storPoolName = "dfltstorpool";
        private String rscNameSuffixToTest = RscLayerSuffixes.SUFFIX_CACHE_META;
        private long vlmSizeInKib;

        private SizeTest(long vlmSizeInKibRef)
        {
            vlmSizeInKib = vlmSizeInKibRef;
        }

        private SizeTest withRscSuffixToTest(String rscSuffixToTestRef)
        {
            rscNameSuffixToTest = rscSuffixToTestRef;
            return this;
        }

        private SizeTest withCacheSize(String cacheSizeRef) throws Exception
        {
            resourceDefinitionTestFactory.get(rscName, true)
                .getProps(SYS_CTX)
                .setProp(ApiConsts.KEY_CACHE_CACHE_SIZE, cacheSizeRef, ApiConsts.NAMESPC_CACHE);
            return this;
        }

        private SizeTest withBlockSize(String blockSizeRef) throws Exception
        {
            resourceDefinitionTestFactory.get(rscName, true)
                .getProps(SYS_CTX)
                .setProp(ApiConsts.KEY_CACHE_BLOCK_SIZE, blockSizeRef, ApiConsts.NAMESPC_CACHE);
            return this;
        }

        private void expect(long expectedSizeInKib) throws Exception
        {
            storPoolTestFactory.get(nodeName, storPoolName, true);
            ResourceDefinition rscDfn = resourceDefinitionTestFactory.get("rsc", true);
            Props rscDfnProps = rscDfn.getProps(SYS_CTX);
            rscDfnProps.setProp(ApiConsts.KEY_CACHE_CACHE_POOL_NAME, storPoolName, ApiConsts.NAMESPC_CACHE);
            rscDfnProps.setProp(ApiConsts.KEY_CACHE_META_POOL_NAME, storPoolName, ApiConsts.NAMESPC_CACHE);

            Resource rsc = resourceTestFactory.builder("node", "rsc")
                .setLayerStack(Arrays.asList(DeviceLayerKind.CACHE, DeviceLayerKind.STORAGE))
                .build();
            Volume vlm = volumeTestFactory.builder("node", "rsc")
                .setSize(vlmSizeInKib)
                .build();

            AbsRscLayerObject<Resource> layerData = rsc.getLayerData(SYS_CTX);
            assertEquals(CacheRscData.class, layerData.getClass());
            CacheRscData<Resource> cacheData = (CacheRscData<Resource>) layerData;
            CacheVlmData<Resource> cacheVlmData = cacheData.getVlmLayerObjects().get(vlm.getVolumeNumber());

            layerSizeHelper.calculateSize(SYS_CTX, cacheVlmData);

            assertEquals(
                expectedSizeInKib,
                cacheVlmData.getChildBySuffix(rscNameSuffixToTest).getUsableSize()
            );

            // cleanup for the next run
            rsc.delete(SYS_CTX);
            rscDfn.getProps(SYS_CTX).clear();
        }
    }
}
