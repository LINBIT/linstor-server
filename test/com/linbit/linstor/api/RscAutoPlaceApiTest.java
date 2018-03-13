package com.linbit.linstor.api;

import javax.inject.Inject;
import com.linbit.linstor.Node;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.Resource;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.ResourceDefinitionData;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.StorPoolData;
import com.linbit.linstor.StorPoolDefinitionData;
import com.linbit.linstor.StorPoolName;
import com.linbit.linstor.Volume;
import com.linbit.linstor.VolumeNumber;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.CtrlRscApiCallHandler;
import com.linbit.linstor.core.CtrlRscAutoPlaceApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.storage.LvmDriver;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RscAutoPlaceApiTest extends ApiTestBase
{
    private static final long KB = 1;
    private static final long MB = 1_000 * KB;
    private static final long GB = 1_000 * MB;
    private static final long TB = 1_000 * GB;

    private static final String TEST_RSC_NAME = "TestRsc";
    private static final int TEST_TCP_PORT_NR = 8000;

    private static final int MINOR_NR_MIN = 1000;
    private static final AtomicInteger MINOR_GEN = new AtomicInteger(MINOR_NR_MIN);

    @Inject private Provider<CtrlRscAutoPlaceApiCallHandler> rscAutoPlaceApiCallHandlerProvider;
    @Inject private Provider<CtrlRscApiCallHandler> rscApiCallHandlerProvider;

    @Mock
    protected Peer mockSatellite;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        createRscDfn(TEST_RSC_NAME, TEST_TCP_PORT_NR);
        MINOR_GEN.set(MINOR_NR_MIN);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void basicTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                ApiConsts.WARN_NOT_CONNECTED, // stlt1
                ApiConsts.CREATED, // stlt1, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt1, rsc, vlm
                ApiConsts.WARN_NOT_CONNECTED, // sttl1 (still...)
                ApiConsts.WARN_NOT_CONNECTED, // stlt2
                ApiConsts.CREATED, // stlt2, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt2, rsc, vlm
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)
        );
        expectDeployed(
            "slow1",
            TEST_RSC_NAME,
            "stlt1", "stlt2"
        );
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void preferredStorPoolNotEnoughSpaceTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                ApiConsts.FAIL_NOT_ENOUGH_NODES
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)
            .setStorPool("fast1")
        );
        expectNotDeployed(TEST_RSC_NAME);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void preferredStorPoolSuccessTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                ApiConsts.WARN_NOT_CONNECTED, // stlt1
                ApiConsts.CREATED, // stlt1, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt1, rsc, vlm
                ApiConsts.WARN_NOT_CONNECTED, // sttl1 (still...)
                ApiConsts.WARN_NOT_CONNECTED, // stlt2
                ApiConsts.CREATED, // stlt2, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt2, rsc, vlm
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)
            .setStorPool("fast1")
        );

        expectDeployed(
            "fast1",
            TEST_RSC_NAME,
            "stlt1", "stlt2"
        );
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void doNotPlaceWithRscTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                ApiConsts.WARN_NOT_CONNECTED, // stlt1
                ApiConsts.CREATED, // stlt1, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt1, rsc, vlm
                ApiConsts.WARN_NOT_CONNECTED, // sttl1 (still...)
                ApiConsts.WARN_NOT_CONNECTED, // stlt2
                ApiConsts.CREATED, // stlt2, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt2, rsc, vlm
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)

            .doNotPlaceWith("avoid1")

            .addRscDfn("avoid1", TEST_TCP_PORT_NR + 1)
            .addVlmDfn("avoid1", 0, 2 * TB)
            .addRsc("avoid1", "slow1", "stlt1", "stlt2")
        );

        expectDeployed(
            "fast1",
            TEST_RSC_NAME,
            "stlt1", "stlt2"
        );
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void notEnoughNodesTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                3,
                ApiConsts.FAIL_NOT_ENOUGH_NODES
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)
        );
        expectNotDeployed(TEST_RSC_NAME);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void doNotPlaceWithRscAndStorPoolTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                ApiConsts.FAIL_NOT_ENOUGH_NODES
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 5 * TB)

            .doNotPlaceWith("avoid1")
            .setStorPool("slow1")

            .addRscDfn("avoid1", TEST_TCP_PORT_NR + 1)
            .addVlmDfn("avoid1", 0, 2 * TB)
            .addRsc("avoid1", "slow1", "stlt1", "stlt2")
        );
        expectNotDeployed(TEST_RSC_NAME);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void doNotPlaceWithRscRegexTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                ApiConsts.WARN_NOT_CONNECTED, // stlt1
                ApiConsts.CREATED, // stlt1, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt1, rsc, vlm
                ApiConsts.WARN_NOT_CONNECTED, // sttl1 (still...)
                ApiConsts.WARN_NOT_CONNECTED, // stlt2
                ApiConsts.CREATED, // stlt2, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt2, rsc, vlm
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)

            .setDoNotPlaceWithRegex("avoid.*")

            .addRscDfn("avoid1", TEST_TCP_PORT_NR + 1)
            .addVlmDfn("avoid1", 0, 2 * TB)
                .addRsc("avoid1", "slow1", "stlt1", "stlt2")
            .addRscDfn("avoid2", TEST_TCP_PORT_NR + 2)
            .addVlmDfn("avoid2", 0, 2 * TB)
                .addRsc("avoid2", "slow2", "stlt1", "stlt2")
        );

        expectDeployed(
            "fast1",
            TEST_RSC_NAME,
            "stlt1", "stlt2"
        );
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void doNotPlaceWithRscSimpleRegexPrefixTest() throws Exception
    {
        evaluateTest(
            new RscAutoPlaceApiCall(
                TEST_RSC_NAME,
                2,
                ApiConsts.WARN_NOT_CONNECTED, // stlt1
                ApiConsts.CREATED, // stlt1, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt1, rsc, vlm
                ApiConsts.WARN_NOT_CONNECTED, // sttl1 (still...)
                ApiConsts.WARN_NOT_CONNECTED, // stlt2
                ApiConsts.CREATED, // stlt2, rsc
                ApiConsts.MASK_VLM | ApiConsts.CREATED, // stlt2, rsc, vlm
                ApiConsts.CREATED // rsc autoplace
            )
            .stltBuilder("stlt1")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .stltBuilder("stlt2")
                .addStorPool("slow1", 10 * TB)
                .addStorPool("slow2", 20 * TB)
                .addStorPool("fast1", 100 * GB)
                .build()
            .addVlmDfn(TEST_RSC_NAME, 0, 50 * GB)

            .setDoNotPlaceWithRegex("avoid") // no trailing ".*"

            .addRscDfn("avoid1", TEST_TCP_PORT_NR + 1)
            .addVlmDfn("avoid1", 0, 2 * TB)
                .addRsc("avoid1", "slow1", "stlt1", "stlt2")
            .addRscDfn("avoid2", TEST_TCP_PORT_NR + 2)
            .addVlmDfn("avoid2", 0, 2 * TB)
                .addRsc("avoid2", "slow2", "stlt1", "stlt2")
        );

        expectDeployed(
            "fast1",
            TEST_RSC_NAME,
            "stlt1", "stlt2"
        );
    }

    private void expectDeployed(
        String storPoolNameStr,
        String rscNameStr,
        String... nodeNameStrs
    )
        throws Exception
    {
        StorPoolName storPoolName = new StorPoolName(storPoolNameStr);
        ResourceName rscName = new ResourceName(rscNameStr);

        for (String nodeNameStr : nodeNameStrs)
        {
            Node node = nodesMap.get(new NodeName(nodeNameStr));
            Resource rsc = node.getResource(SYS_CTX, rscName);
            assertNotNull(rsc);

            Iterator<Volume> vlmIt = rsc.iterateVolumes();
            assertTrue(vlmIt.hasNext());

            while (vlmIt.hasNext())
            {
                Volume vlm = vlmIt.next();
                assertEquals(storPoolName, vlm.getStorPool(SYS_CTX).getName());
            }
        }
    }

    private void expectNotDeployed(String rscNameStr) throws Exception
    {
        ResourceName rscName = new ResourceName(rscNameStr);

        ResourceDefinition rscDfn = rscDfnMap.get(rscName);

        assertThat(rscDfn.getResourceCount()).isEqualTo(0);
    }


    private ResourceDefinitionData createRscDfn(String rscNameStr, int tcpPort)
        throws Exception
    {
        ResourceDefinitionData rscDfn = resourceDefinitionDataFactory.create(
            BOB_ACC_CTX,
            new ResourceName(rscNameStr),
            tcpPort,
            null,
            "NotTellingYou",
            ResourceDefinition.TransportType.IP
        );

        rscDfnMap.put(rscDfn.getName(), rscDfn);

        return rscDfn;
    }

    private class RscAutoPlaceApiCall extends AbsApiCallTester
    {
        private final String rscNameStr;
        private final int placeCount;

        private final List<String> doNotPlaceWithRscList = new ArrayList<>();
        private String forceStorPool = null;
        private String doNotPlaceWithRscRegexStr = null;

        RscAutoPlaceApiCall(
            String rscNameStrRef,
            int placeCountRef,
            long... expectedRetCodes
        )
        {
            super(
                ApiConsts.MASK_RSC,
                ApiConsts.MASK_CRT,
                expectedRetCodes
            );
            rscNameStr = rscNameStrRef;
            placeCount = placeCountRef;
        }

        public RscAutoPlaceApiCall setDoNotPlaceWithRegex(String doNotPlaceWithRscRegexStrRef)
        {
            doNotPlaceWithRscRegexStr = doNotPlaceWithRscRegexStrRef;
            return this;
        }

        RscAutoPlaceApiCall setStorPool(String forceStorPoolRef)
        {
            forceStorPool = forceStorPoolRef;
            return this;
        }

        RscAutoPlaceApiCall doNotPlaceWith(String... doNotPlaceWithRsc)
        {
            doNotPlaceWithRscList.addAll(Arrays.asList(doNotPlaceWithRsc));
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return rscAutoPlaceApiCallHandlerProvider.get().autoPlace(
                rscNameStr,
                placeCount,
                forceStorPool,
                doNotPlaceWithRscList,
                doNotPlaceWithRscRegexStr
            );
        }

        SatelliteBuilder stltBuilder(String stltName) throws Exception
        {
            NodeData stlt = nodeDataFactory.getInstance(
                BOB_ACC_CTX,
                new NodeName(stltName),
                Node.NodeType.SATELLITE,
                null,
                true,
                true
            );

            stlt.setPeer(SYS_CTX, mockSatellite);
            nodesMap.put(stlt.getName(), stlt);

            return new SatelliteBuilder(this, stlt);
        }

        RscAutoPlaceApiCall addVlmDfn(String rscNameStrRef, int vlmNrRef, long sizeRef) throws Exception
        {
            ResourceName rscName = new ResourceName(rscNameStrRef);
            ResourceDefinition rscDfn = rscDfnMap.get(rscName);

            volumeDefinitionDataFactory.create(
                BOB_ACC_CTX,
                rscDfn,
                new VolumeNumber(vlmNrRef),
                MINOR_GEN.incrementAndGet(),
                sizeRef,
                null
            );

            return this;
        }

        RscAutoPlaceApiCall addRscDfn(String rscNameStrRef, int tcpPortRef) throws Exception
        {
            createRscDfn(rscNameStrRef, tcpPortRef);
            return this;
        }


        RscAutoPlaceApiCall addRsc(String rscNameStrRef, String storPool, String... stltNameStrs) throws Exception
        {
            Map<String, String> rscPropsMap = new TreeMap<>();
            rscPropsMap.put(ApiConsts.KEY_STOR_POOL_NAME, storPool);
            for (String stltNameStr : stltNameStrs)
            {
                rscApiCallHandlerProvider.get().createResource(
                    stltNameStr,
                    rscNameStrRef,
                    Collections.emptyList(),
                    rscPropsMap,
                    Collections.emptyList()
                );
            }
            return this;
        }
    }


    private class SatelliteBuilder
    {
        private final RscAutoPlaceApiCall parent;
        private final NodeData stlt;

        SatelliteBuilder(RscAutoPlaceApiCall parentRef, NodeData stltRef)
        {
            parent = parentRef;
            stlt = stltRef;
        }

        SatelliteBuilder addStorPool(String storPoolName, long storPoolSize) throws Exception
        {
            StorPoolDefinitionData storPoolDfn = storPoolDefinitionDataFactory.getInstance(
                BOB_ACC_CTX,
                new StorPoolName(storPoolName),
                true,
                false
            );

            storPoolDfnMap.put(storPoolDfn.getName(), storPoolDfn);

            StorPoolData storPool = storPoolDataFactory.getInstance(
                BOB_ACC_CTX,
                stlt,
                storPoolDfn,
                LvmDriver.class.getSimpleName(),
                true,
                false
            );

            storPool.setRealFreeSpace(SYS_CTX, storPoolSize);

            return this;
        }

        public RscAutoPlaceApiCall build() throws Exception
        {
            return parent;
        }
    }
}
