package com.linbit.linstor.core.objects;

import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The resource definition offers three "diskful" views and {@link Resource} two predicates, which agree on one
 * rule: NVMe and EBS initiators have no <b>local</b> disk (so they are diskless for the flag based lookups) but
 * they are the DRBD <b>diskful</b> peers (so they count for the UpToDate winner), while EBS and NVMe targets
 * carry the disk but never run DRBD.
 */
public class ResourceDiskfulLookupsTest extends GenericDbBase
{
    private static final String RSC_NAME = "rsc";
    private static final String PLAIN_NODE = "plain";
    private static final String DISKLESS_NODE = "diskless";
    private static final String EBS_INIT_NODE = "ebsinit";
    private static final String NVME_INIT_NODE = "nvmeinit";
    private static final String EBS_TARGET_NODE = "ebstarget";

    private static final List<DeviceLayerKind> DRBD_STACK = Arrays.asList(
        DeviceLayerKind.DRBD,
        DeviceLayerKind.STORAGE
    );
    private static final List<DeviceLayerKind> NVME_STACK = Arrays.asList(
        DeviceLayerKind.DRBD,
        DeviceLayerKind.NVME,
        DeviceLayerKind.STORAGE
    );
    private static final List<DeviceLayerKind> STORAGE_ONLY_STACK = Arrays.asList(DeviceLayerKind.STORAGE);

    private ResourceDefinition rscDfn;
    private Resource plain;
    private Resource diskless;
    private Resource ebsInit;
    private Resource nvmeInit;
    private Resource ebsTarget;

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();

        nodeTestFactory.builder(EBS_TARGET_NODE).setNodeType(Node.Type.EBS_TARGET).build();

        plain = createRsc(PLAIN_NODE, DRBD_STACK);
        diskless = createRsc(DISKLESS_NODE, DRBD_STACK, Resource.Flags.DRBD_DISKLESS);
        // an EBS initiator with a DRBD layer makes RscDrbdLayerHelper look up its EBS target via the storage pools'
        // availability zone, which needs remotes and pools this test does not model; the flag based lookups do
        // not depend on the layer data anyway
        ebsInit = createRsc(EBS_INIT_NODE, STORAGE_ONLY_STACK, Resource.Flags.EBS_INITIATOR);
        nvmeInit = createRsc(NVME_INIT_NODE, DRBD_STACK, Resource.Flags.NVME_INITIATOR);
        ebsTarget = createRsc(EBS_TARGET_NODE, DRBD_STACK);
        rscDfn = plain.getResourceDefinition();

        commit();
    }

    @Test
    public void diskfulResourcesAreThoseWithLocalDisk()
    {
        assertThat(rscDfn.getDiskfulResources()).containsExactlyInAnyOrder(plain, ebsTarget);
        assertThat(rscDfn.getDiskfulCount()).isEqualTo(2);
    }

    @Test
    public void disklessResourcesIncludeInitiators()
    {
        assertThat(rscDfn.getDisklessResources()).containsExactlyInAnyOrder(diskless, ebsInit, nvmeInit);
    }

    @Test
    public void notDeletedDiskfulAgreesWithDiskful()
    {
        // the third lookup used the older definition (EBS initiators counted as diskful) for a long time
        assertThat(rscDfn.getNotDeletedDiskful()).containsExactlyInAnyOrder(plain, ebsTarget);
        assertThat(rscDfn.getNotDeletedDiskfulCount()).isEqualTo(2);
        assertThat(rscDfn.getNotDeletedDiskfulCountExcluding(plain)).isEqualTo(1);
    }

    @Test
    public void drbdDiskfulResourcesAreThePeersThatCanRunSetGi()
    {
        // the NVMe initiator backs DRBD with the attached disk; the target is a special satellite that never runs
        // DRBD; the EBS initiator of this fixture has no DRBD layer at all (see setUp) and is therefore out as well
        assertThat(rscDfn.getDrbdDiskfulResources()).containsExactlyInAnyOrder(plain, nvmeInit);
    }

    @Test
    public void isDrbdDiskfulFollowsTheSameRule()
    {
        assertThat(plain.isDrbdDiskful()).isTrue();
        assertThat(nvmeInit.isDrbdDiskful()).isTrue();
        assertThat(diskless.isDrbdDiskful()).isFalse();
        assertThat(ebsTarget.isDrbdDiskful()).isFalse();
        // no DRBD layer data in this fixture, see setUp
        assertThat(ebsInit.isDrbdDiskful()).isFalse();
    }

    @Test
    public void flagsCheckWithoutLayerStackCannotTellNvmeTargetsApart()
    {
        // the flags-only variant is used while the layer tree is still being built; without a stack it can only
        // rely on flags and node type
        assertThat(plain.isDrbdDiskfulFlagsCheck(null)).isTrue();
        assertThat(ebsInit.isDrbdDiskfulFlagsCheck(null)).isTrue();
        assertThat(nvmeInit.isDrbdDiskfulFlagsCheck(null)).isTrue();
        assertThat(diskless.isDrbdDiskfulFlagsCheck(null)).isFalse();
        assertThat(ebsTarget.isDrbdDiskfulFlagsCheck(null)).isFalse();
    }

    @Test
    public void flagsCheckWithNvmeStackExcludesTheTarget()
    {
        // in an NVMe stack a resource without the initiator flag is the NVMe target, which does not run DRBD
        assertThat(plain.isDrbdDiskfulFlagsCheck(NVME_STACK)).isFalse();
        assertThat(nvmeInit.isDrbdDiskfulFlagsCheck(NVME_STACK)).isTrue();
        assertThat(diskless.isDrbdDiskfulFlagsCheck(NVME_STACK)).isFalse();
        // a stack without NVMe changes nothing for a plain resource
        assertThat(plain.isDrbdDiskfulFlagsCheck(DRBD_STACK)).isTrue();
    }

    private Resource createRsc(String nodeName, List<DeviceLayerKind> layerStack, Resource.Flags... flags)
        throws Exception
    {
        return resourceTestFactory.builder(nodeName, RSC_NAME)
            .setFlags(flags)
            .setLayerStack(layerStack)
            .build();
    }
}
