package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.objects.AutoSelectorConfig;
import com.linbit.linstor.core.objects.AutoUnselectorConfig;
import com.linbit.linstor.core.objects.AutoUnselectorConfig.CfgBuilder;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceGroup;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AutoUnplacerTest extends GenericDbBase
{
    private static final String DFLT_STOR_POOL = "DfltStorPool";
    private static final String DFLT_DISKLESS_STOR_POOL = "DfltDisklessStorPool";
    private static final String RSC_NAME_STR = "testRsc";
    private static final long SIZE_100_MB = 100 * 1024;
    private static final String DFLT_RSC_GRP = InternalApiConsts.DEFAULT_RSC_GRP_NAME;

    private VlmCfg[] vlmConfigs;

    @Inject private AutoUnplacer autoUnplacer;

    public AutoUnplacerTest()
    {
    }

    @Before
    public void setup() throws Exception
    {
        setUpAndEnterScope();

        vlmConfigs = new VlmCfg[] {
            new VlmCfg() };
    }

    @Test
    public void removeFromAllEqualTest() throws Exception
    {
        Resource rsc = createRsc("node1");
        createRsc("node2");
        createRsc("node3");

        expectUnplaced(rsc);
    }

    @Test
    public void removeFromAllEqualWithDisklessTest() throws Exception
    {
        /*
         * Diskless storage pools have Long.MAX_VALUE free and total capacity. These values should not throw the
         * AutoUnplacer off...
         */
        Resource rsc = createRsc("node1");
        createRsc("node2");
        createRsc("node3");
        createDisklessRsc("node4");

        expectUnplaced(rsc);
    }

    @Test
    public void removeSimpleDisklessTest() throws Exception
    {
        // expect to delete testrsc2 from node2 since testrsc on node4 has only one resource hosted, while node2 has 2
        createRsc("node1");
        createRsc("node2");
        createRsc("node3");
        createDisklessRsc("node4");

        createRsc("node1", "testrsc2");
        Resource rsc = createDisklessRsc("node2", "testrsc2");

        expectUnplacedDiskless(rsc);
    }

    @Test
    public void removeLowestSpaceLeftTest() throws Exception
    {
        createRsc("node1");
        Resource rsc = createRsc("node2");
        createRsc("node3");

        long totalCap = 1_000_000;

        long freeSpaceLow = 100;
        long freeSpaceHigh = 100_000;

        setSpCapacity("node1", freeSpaceHigh, totalCap);
        setSpCapacity("node2", freeSpaceLow, totalCap);
        setSpCapacity("node3", freeSpaceHigh, totalCap);

        expectUnplaced(rsc);
    }

    @Test
    public void removeSimpleViolationInReplOnSameTest() throws Exception
    {
        createRsc("node1");
        createRsc("node2");
        Resource rsc = createRsc("node3");

        setNodeProp("node1", "Aux/site", "a");
        setNodeProp("node2", "Aux/site", "a");
        setNodeProp("node3", "Aux/site", "b");

        setReplicasOnSame("Aux/site");

        expectUnplaced(rsc);
    }

    @Test
    public void removeSimpleViolationInReplOnDifferentTest() throws Exception
    {
        Resource rsc = createRsc("node1");
        createRsc("node2");
        createRsc("node3");

        setNodeProp("node1", "Aux/site", "a");
        setNodeProp("node2", "Aux/site", "a");
        setNodeProp("node3", "Aux/site", "b");

        setReplicasOnDifferent("Aux/site");

        expectUnplaced(rsc);
    }

    @Test
    public void removeSimpleViolationInReplOnDifferentWithFixedRscTest() throws Exception
    {
        Resource fixedRsc = createRsc("node1");
        Resource rsc = createRsc("node2");
        createRsc("node3");

        setNodeProp("node1", "Aux/site", "a");
        setNodeProp("node2", "Aux/site", "a");
        setNodeProp("node3", "Aux/site", "b");

        setReplicasOnSame("Aux/site");

        expectUnplaced(Collections.singleton(fixedRsc), rsc);
    }

    @Test
    public void removeSimpleViolationInXReplOnDifferentRscTest() throws Exception
    {
        Resource rsc = createRsc("nodeA1");
        createRsc("nodeA2");
        createRsc("nodeA3");
        createRsc("nodeB1");
        createRsc("nodeB2");

        setNodeProp("nodeA1", "Aux/site", "a");
        setNodeProp("nodeA2", "Aux/site", "a");
        setNodeProp("nodeA3", "Aux/site", "a");

        setNodeProp("nodeB1", "Aux/site", "b");
        setNodeProp("nodeB2", "Aux/site", "b");

        putXReplicasOnDifferent(2, "Aux/site");

        expectUnplaced(rsc);
    }

    @Test
    public void cornerCaseNoResourcesTest() throws Exception
    {
        ResourceDefinition rscDfn = resourceDefinitionTestFactory.create(RSC_NAME_STR);
        expectUnplaced(
            new AutoUnselectorConfig.CfgBuilder(rscDfn),
            null
        );
    }

    @Test
    public void cornerCaseSingleDiskfulResourceTest() throws Exception
    {
        Resource rsc = createRsc("node1");
        expectUnplaced(rsc);
    }

    @Test
    public void cornerCaseSingleDisklessResourceTest() throws Exception
    {
        // having only a diskless resource with 0 diskful peers should never happen. however we should still be able to
        // get out of this situation.
        Resource rsc = createDisklessRsc("node1");
        expectUnplaced(
            new AutoUnselectorConfig.CfgBuilder(rsc.getResourceDefinition()),
            null
        );
        expectUnplacedDiskless(rsc);
    }

    private void setReplicasOnSame(String... replicasOnSameArrRef) throws Exception
    {
        setReplicasOnSameOnRg(DFLT_RSC_GRP, replicasOnSameArrRef);
    }

    private void setReplicasOnSameOnRg(String rscGrpNameRef, String... replicasOnSameArrRef) throws Exception
    {
        ResourceGroup rscGrp = resourceGroupTestFactory.get(rscGrpNameRef, false);
        rscGrp.getAutoPlaceConfig()
            .applyChanges(
                new AutoSelectFilterBuilder().setReplicasOnSameList(Arrays.asList(replicasOnSameArrRef)).build()
            );
    }

    private void setReplicasOnDifferent(String... replicasOnDiffArrRef) throws Exception
    {
        setReplicasOnDifferentOnRg(DFLT_RSC_GRP, replicasOnDiffArrRef);
    }

    private void setReplicasOnDifferentOnRg(String rscGrpNameRef, String... replicasOnDiffArrRef) throws Exception
    {
        ResourceGroup rscGrp = resourceGroupTestFactory.get(rscGrpNameRef, false);
        rscGrp.getAutoPlaceConfig()
            .applyChanges(
                new AutoSelectFilterBuilder().setReplicasOnDifferentList(Arrays.asList(replicasOnDiffArrRef)).build()
            );
    }

    private void putXReplicasOnDifferent(int xRef, String propRef) throws Exception
    {
        putXReplicasOnDifferentOnRg(DFLT_RSC_GRP, xRef, propRef);
    }

    private void putXReplicasOnDifferentOnRg(String rscGrpNameRef, int xRef, String propRef) throws Exception
    {
        ResourceGroup rscGrp = resourceGroupTestFactory.get(rscGrpNameRef, false);

        AutoSelectorConfig cfg = rscGrp.getAutoPlaceConfig();
        Map<String, Integer> xReplOnDiff = new TreeMap<>(cfg.getXReplicasOnDifferentMap(SYS_CTX));
        xReplOnDiff.put(propRef, xRef);

        cfg.applyChanges(
            new AutoSelectFilterBuilder().setXReplicasOnDifferentMap(xReplOnDiff).build()
        );
    }

    private void setNodeProp(String nodeNameRef, String propKeyRef, String propValueRef) throws Exception
    {
        nodeTestFactory.get(nodeNameRef, false).getProps(SYS_CTX).setProp(propKeyRef, propValueRef);
    }

    private void expectUnplaced(Resource expectedRscRef) throws AccessDeniedException
    {
        expectUnplaced(Collections.emptyList(), expectedRscRef);
    }

    private void expectUnplaced(Collection<Resource> fixedResourcesRef, Resource expectedRscRef)
        throws AccessDeniedException
    {
        expectUnplaced(
            new AutoUnselectorConfig.CfgBuilder(expectedRscRef.getResourceDefinition())
                .setFilterForFixedResources(fixedResourcesRef),
            expectedRscRef
        );
    }

    private void expectUnplacedDiskless(Resource expectedRscRef) throws AccessDeniedException
    {
        expectUnplacedDiskless(Collections.emptyList(), expectedRscRef);
    }

    private void expectUnplacedDiskless(Collection<Resource> fixedResourcesRef, Resource expectedRscRef)
        throws AccessDeniedException
    {
        expectUnplaced(
            new AutoUnselectorConfig.CfgBuilder(expectedRscRef.getResourceDefinition())
                .setFilterForFixedResources(fixedResourcesRef)
                .setDisklessFilter(),
            expectedRscRef
        );
    }

    private void expectUnplaced(CfgBuilder setFilterForFixedResourcesRef, @Nullable Resource expectedRscRef)
        throws AccessDeniedException
    {
        expectUnplaced(setFilterForFixedResourcesRef.build(SYS_CTX), expectedRscRef);
    }

    private void expectUnplaced(AutoUnselectorConfig autoUnplaceCfgRef, @Nullable Resource expectedRscRef)
    {
        @Nullable Resource actualRsc = autoUnplacer.unplace(autoUnplaceCfgRef);
        assertEquals(expectedRscRef, actualRsc);
    }

    private Resource createRsc(String nodeName) throws Exception
    {
        return createRsc(nodeName, RSC_NAME_STR);
    }

    private Resource createRsc(String nodeNameRef, String rscNameStrRef) throws Exception
    {
        Resource rsc = resourceTestFactory.create(nodeNameRef, rscNameStrRef);

        for (int vlmNr = 0; vlmNr < vlmConfigs.length; vlmNr++)
        {
            VlmCfg vlmCfg = vlmConfigs[vlmNr];
            volumeDefinitionTestFactory.get(rscNameStrRef, vlmNr, vlmCfg.size, true);
            rsc.getProps(SYS_CTX).setProp(ApiConsts.KEY_STOR_POOL_NAME, vlmCfg.storPoolStr);
            storPoolTestFactory.get(nodeNameRef, vlmCfg.storPoolStr, true);
            volumeTestFactory.get(nodeNameRef, rscNameStrRef, vlmNr, true);
        }
        return rsc;
    }

    private Resource createDisklessRsc(String nodeName) throws Exception
    {
        return createDisklessRsc(nodeName, RSC_NAME_STR);
    }

    private Resource createDisklessRsc(String nodeNameRef, String rscNameStrRef) throws Exception
    {
        Resource rsc = resourceTestFactory.builder(nodeNameRef, rscNameStrRef)
            .setFlags(new Resource.Flags[]
            {
                Resource.Flags.DRBD_DISKLESS
            })
            .build();


        for (int vlmNr = 0; vlmNr < vlmConfigs.length; vlmNr++)
        {
            VlmCfg vlmCfg = vlmConfigs[vlmNr];
            volumeDefinitionTestFactory.get(rscNameStrRef, vlmNr, vlmCfg.size, true);
            rsc.getProps(SYS_CTX).setProp(ApiConsts.KEY_STOR_POOL_NAME, vlmCfg.disklessStorPoolStr);
            @Nullable StorPool storPool = storPoolTestFactory.get(nodeNameRef, vlmCfg.disklessStorPoolStr, false);
            if (storPool == null)
            {
                storPoolTestFactory.builder(nodeNameRef, vlmCfg.disklessStorPoolStr)
                    .setDriverKind(DeviceProviderKind.DISKLESS)
                    .build();
                setSpCapacity(nodeNameRef, vlmCfg.disklessStorPoolStr, Long.MAX_VALUE, Long.MAX_VALUE);
            }
            volumeTestFactory.get(nodeNameRef, rscNameStrRef, vlmNr, true);
        }
        return rsc;
    }

    private void setSpCapacity(String nodeNameRef, long freeSpaceRef, long totalCapacityRef) throws Exception
    {
        setSpCapacity(nodeNameRef, DFLT_STOR_POOL, freeSpaceRef, totalCapacityRef);
    }

    private void setSpCapacity(String nodeNameRef, String storPoolRef, long freeSpaceRef, long totalCapacityRef)
        throws Exception
    {
        storPoolTestFactory.get(nodeNameRef, storPoolRef, false)
            .getFreeSpaceTracker()
            .setCapacityInfo(SYS_CTX, freeSpaceRef, totalCapacityRef);
    }

    private static class VlmCfg
    {
        private long size = SIZE_100_MB;
        private String storPoolStr = DFLT_STOR_POOL;
        private String disklessStorPoolStr = DFLT_DISKLESS_STOR_POOL;
    }
}
