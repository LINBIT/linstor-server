package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.SharedStorPoolManager.UpdateSet;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.Snapshot;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.TestAccessContextProvider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SharedStorPoolManagerTest extends GenericDbBase
{
    private static final long GiB = 1024 * 1024; // base-unit in KiB

    private SharedStorPoolManager sharedStorPoolMgr;

    @Before
    public void setup() throws Exception
    {
        setUpAndEnterScope();

        sharedStorPoolMgr = new SharedStorPoolManager(
            TestAccessContextProvider.SYS_CTX,
            errorReporter,
            transObjFactory,
            transMgrProvider
        );

        resourceGroupTestFactory.initDfltRscGrp();
        volumeTestFactory.setDfltStorPoolData("spShared")
            .setDfltVlmSize(1 * GiB);

    }

    /*
     * Single resource in spShared
     *
     * Acquire lock for rsc (Granted)
     * Release spShared (empty, done)
     */
    @Test
    public void simpleLockAndRelease() throws Exception
    {
        StorPool sp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();

        Volume vlm = volumeTestFactory.builder("node", "rsc").build();
        Resource rsc = vlm.getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sp));

        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc)); // granted
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sp),
            emptyUpdateSet()
        );
        assertFalse(sharedStorPoolMgr.isActive(sp));
    }

    /*
     * single resource within a spShared
     *
     * Acquire lock for rsc (granted)
     * Acquire lock for rsc (rejected - still in progress)
     * Acquire lock for rsc (rejected - still in progress)
     * Release spShared (rsc can be re-updated)
     * Release spShared (empty, done)
     * - rsc was rejected 2 times, but "grouped" later for only one update
     */
    @Test
    public void reRequestedLock() throws Exception
    {
        StorPool sp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();

        Volume vlm = volumeTestFactory.builder("node", "rsc").build();
        Resource rsc = vlm.getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sp));

        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc));
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // same rsc requests same lock, but does not get it, gets queued again
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc));
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // same rsc requests same lock, gets rejected again
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc));
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sp),
            updateSetBuilder().add(rsc).build()
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sp),
            emptyUpdateSet() // done, previous 2x rejects were grouped together
        );
        assertFalse(sharedStorPoolMgr.isActive(sp));
    }

    /*
     * single resource within a spShared
     *
     * Acquire lock for rsc (granted)
     * Acquire lock for rsc (rejected - still in progress)
     * Acquire lock for rsc (rejected - still in progress)
     * Release spShared (rsc can be re-updated)
     * Release spShared (empty, done)
     * - rsc was rejected 2 times, but "grouped" later for only one update
     */
    @Test
    public void groupRejectedUpdates() throws Exception
    {
        StorPool sp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();

        Volume vlm = volumeTestFactory.builder("node", "rsc").build();
        Resource rsc = vlm.getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sp));

        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc));
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // first rejected attempt
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc));
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // second rejected attempt
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc));
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // update once
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sp),
            rsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // no more updates, done
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sp)
            // empty
        );
        assertFalse(sharedStorPoolMgr.isActive(sp));
    }

    /*
     * simpleRsc based on non-shared SP
     * sharedRsc based on shared SP
     *
     * Acquire lock for simpleRsc (always granted)
     * Acquire lock for simpleRsc (always granted)
     * Acquire lock for sharedRsc (granted)
     * Acquire lock for simpleRsc (always granted)
     * Acquire lock for sharedRsc (rejected, old update still in progress)
     * Release spShared (sharedRsc can be started now)
     * Release spShared (no rsc in queue)
     * Acquire lock for simpleRsc (always granted)
     */
    @Test
    public void nonSharedWhileLock() throws Exception
    {
        StorPool sharedSp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool simpleSp = storPoolTestFactory.builder("node", "spSimple")
            .build();

        Volume sharedVlm = volumeTestFactory.builder("node", "rsc").build();
        Resource sharedRsc = sharedVlm.getAbsResource();

        Volume simpleVlm = volumeTestFactory.builder("node", "rsc2")
            .setStorPoolData(simpleSp).build();
        Resource simpleRsc = simpleVlm.getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertTrue(sharedStorPoolMgr.requestSharedLock(simpleRsc));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // repeatable, no locks for simple storPools
        assertTrue(sharedStorPoolMgr.requestSharedLock(simpleRsc));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertTrue(sharedStorPoolMgr.requestSharedLock(sharedRsc));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // still no locks for simple storPools
        assertTrue(sharedStorPoolMgr.requestSharedLock(simpleRsc));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // but lock for shared SP
        assertFalse(sharedStorPoolMgr.requestSharedLock(sharedRsc));// first request still in progress, lock rejected
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp),
            sharedRsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp)
            // empty, done
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // still no locks for simple storPools
        assertTrue(sharedStorPoolMgr.requestSharedLock(simpleRsc));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));
    }

    /*
     * All resources from same node:
     *
     * Acquire lock for rsc1 (granted)
     * Acquire lock for rsc2 (rejected)
     * Acquire lock for rsc3 (rejected)
     * Acquire lock for rsc4 (rejected)
     * Release lock spShared (rsc2,rsc3 and rsc4 can start now)
     * Release lock spShared (no new resources)
     * End
     */
    @Test
    public void multipleRscsWaitingForSameLock() throws Exception
    {
        StorPool sharedSp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();

        Resource rsc1 = volumeTestFactory.builder("node", "rsc1").build().getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSp));

        // first come, first serve
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc1 still in progress
        Resource rsc2 = volumeTestFactory.builder("node", "rsc2").build().getAbsResource();
        Resource rsc3 = volumeTestFactory.builder("node", "rsc3").build().getAbsResource();
        Resource rsc4 = volumeTestFactory.builder("node", "rsc4").build().getAbsResource();

        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc3));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc4));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc1 finally finishes
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp),
            rsc2, rsc3, rsc4
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc2-4 finish
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp)
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
    }

    /*
     * Resources from different nodes, all same spShared
     *
     * Acquire lock for n1/rsc1 (granted)
     * Acquire lock for n2/rsc1 (rejected)
     * Acquire lock for n1/rsc2 (rejected)
     * Acquire lock for n2/rsc2 (rejected)
     * Release lock spShared (n2/rsc1 and n2/rsc2 can start now)
     * - n2/rsc1 is the next, but can be combined with n2/rsc2
     * Release lock spShared (n1/rsc2 can start now)
     * Release lock spShared (no new resources)
     * End
     */
    @Test
    public void multipleRscsFromDifferntNodesWaitingForSameLock() throws Exception
    {
        StorPool sharedSpN1 = storPoolTestFactory.builder("n1", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool sharedSpN2 = storPoolTestFactory.builder("n2", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();

        Resource rsc11 = volumeTestFactory.builder("n1", "rsc1").build().getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // first come, first serve
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc11));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // n1/rsc1 still in progress
        Resource rsc21 = volumeTestFactory.builder("n2", "rsc1").build().getAbsResource();
        Resource rsc12 = volumeTestFactory.builder("n1", "rsc2").build().getAbsResource();
        Resource rsc22 = volumeTestFactory.builder("n2", "rsc2").build().getAbsResource();

        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc21));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc12));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc22));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));

        // n1/rsc1 finally finishes
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSpN1),
            rsc21,
            rsc22
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN2));

        // n2/rsc1 and n2/rsc2 done
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSpN2),
            rsc12
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // all done
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSpN1)
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));
    }

    /*
     * rsc1 in spShared
     * rsc2 in spShared2
     * rsc12 in both, spShared and spShared2 (i.e. because of DRBD-metadata)
     *
     * Acquire lock for rsc1 (granted)
     * Acquire lock for rsc2 (granted)
     * Acquire lock for rsc12 (rejected)
     * Release lock spShared2 (no new rsc to progress)
     * Release lock spShared (rsc1 ready to progress
     * Release lock spShared and spShared2
     * End
     */
    @Test
    public void delayMultiLock() throws Exception
    {
        StorPool sharedSp1 = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool sharedSp2 = storPoolTestFactory.builder("node", "spShared2")
            .setFreeSpaceMgrName("shared2")
            .build();

        // in spShared
        Resource rsc1 = volumeTestFactory.builder("node", "rsc1").build().getAbsResource();
        // in spShared2
        Resource rsc2 = volumeTestFactory.builder("node", "rsc2")
            .setStorPoolData(sharedSp2)
            .build().getAbsResource();
        // in spShared1 AND spShared2
        Resource rsc12 = volumeTestFactory.builder("node", "rsc12")
            .setStorPoolData(sharedSp1)
            .putStorPool(".meta", sharedSp2)
            .build().getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc1 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc2 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 has to be delayed
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc12));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc2 finishes, rsc12 still delayed
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp2)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc1 finishes, rsc12 can start
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp1),
            rsc12
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 finishes, all done
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp1, sharedSp2)
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));
    }

    /*
     * rsc1 in spShared
     * rsc2 in spShared2
     * rsc12 in both, spShared and spShared2 (i.e. because of DRBD-metadata)
     *
     * Acquire lock for rsc1 (granted)
     * Acquire lock for rsc12 (rejected)
     * Acquire lock for rsc2 (rejected - lock is already requested by rsc12)
     * - we need to avoid continuously delaying multi-locks
     * Release lock spShared (rsc12 ready for update)
     * Release lock spShared and spShared2 (rsc2 ready to progress)
     * Release lock spShared2
     * End
     */
    @Test
    public void delayMultiLock2() throws Exception
    {
        StorPool sharedSp1 = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool sharedSp2 = storPoolTestFactory.builder("node", "spShared2")
            .setFreeSpaceMgrName("shared2")
            .build();

        // in spShared
        Resource rsc1 = volumeTestFactory.builder("node", "rsc1").build().getAbsResource();
        // in spShared2
        Resource rsc2 = volumeTestFactory.builder("node", "rsc2")
            .setStorPoolData(sharedSp2)
            .build().getAbsResource();
        // in spShared1 AND spShared2
        Resource rsc12 = volumeTestFactory.builder("node", "rsc12")
            .setStorPoolData(sharedSp1)
            .putStorPool(".meta", sharedSp2)
            .build().getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc1 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 gets rejected
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc12));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2)); // although reserved for rsc12

        // rsc2 rejected
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc1 finishes, rsc12 can go
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp1),
            rsc12
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 finishes, rsc2 can start
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp1, sharedSp2),
            rsc2
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc2 finishes, all done
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp2)
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));
    }

    @Test(expected = ImplementationError.class)
    public void releaseUnlockedSharedSp() throws Exception
    {
        StorPool sharedSp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();

        // not locked
        sharedStorPoolMgr.releaseLock(sharedSp);
    }

    @Test()
    public void releaseUnlockedSimpleSp() throws Exception
    {
        StorPool simpleSp = storPoolTestFactory.builder("node", "sp1")
            .build();

        // not locked
        sharedStorPoolMgr.releaseLock(simpleSp); // no error
    }

    /*
     * sp1 = "shared" on node1
     * sp2 = "shared" on node2
     *
     * rsc1 in sp1
     * rsc2 in sp2
     *
     * Acquire lock for rsc1 (granted)
     * Acquire lock for sp2 (rejected)
     * Release lock sp1 (sp2 ready for update)
     * Release lock sp2 (empty, done)
     * End
     *
     */
    @Test
    public void lockStorPool() throws Exception
    {
        StorPool sharedSp1 = storPoolTestFactory.builder("node1", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool sharedSp2 = storPoolTestFactory.builder("node2", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();

        Resource rsc1 = volumeTestFactory.builder("node1", "rsc")
            .setStorPoolData(sharedSp1)
            .build().getAbsResource();
        Resource rsc2 = volumeTestFactory.builder("node2", "rsc")
            .setStorPoolData(sharedSp2)
            .build().getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc1 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        assertFalse(sharedStorPoolMgr.requestSharedLock(sharedSp2));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp1),
            sharedSp2
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sharedSp2)
            // empty, done
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
    }

    /*
     * sp11 == "shared1" on node1
     *
     * sp12 == "shared1" on node2
     * sp22 == "shared2" on node2
     *
     * sp23 == "shared2" on node3
     * sp33 == "shared3" on node3
     *
     * sp34 == "shared3" on node4
     * sp44 == "shared4" on node4
     *
     * sp54 == "shared4" on node5
     *
     * rsc1 in sp11
     * rsc2 in sp12 and sp22
     * # no rsc3
     * rsc4 in sp34 and sp44
     * rsc5 in sp54
     *
     * Acquire lock for rsc1 (granted, locks taken: "shared1")
     * Acquire lock for rsc5 (granted, locks taken: "shared1", "shared4")
     * Acquire lock for rsc2 (rejected, as lock "shared1" is still held by rsc1)
     * Acquire lock for rsc4 (rejected, as lock "shared4" is still held by rsc5)
     * Acquire lock for node3 (rejected, as lock "shared2" is still reserved by rsc2 and "shared3" is reserved by rsc4)
     *
     * Release lock shared1 (rsc2 can start now)
     * Release lock shared2 (empty, node3 still needs "shared3")
     * Release lock shared4 (rsc5 can start now)
     * Release lock shared3 (node3 can start now)
     * Release locks shared3 and shared4 (empty, done)
     */
    @Test
    public void transitiveDelay() throws Exception
    {
        StorPool sp11 = storPoolTestFactory.builder("node1", "sp11")
            .setFreeSpaceMgrName("shared1")
            .build();

        StorPool sp12 = storPoolTestFactory.builder("node2", "sp12")
            .setFreeSpaceMgrName("shared1")
            .build();
        StorPool sp22 = storPoolTestFactory.builder("node2", "sp22")
            .setFreeSpaceMgrName("shared2")
            .build();

        StorPool sp23 = storPoolTestFactory.builder("node3", "sp23")
            .setFreeSpaceMgrName("shared2")
            .build();
        StorPool sp33 = storPoolTestFactory.builder("node3", "sp33")
            .setFreeSpaceMgrName("shared3")
            .build();

        StorPool sp34 = storPoolTestFactory.builder("node4", "sp34")
            .setFreeSpaceMgrName("shared3")
            .build();
        StorPool sp44 = storPoolTestFactory.builder("node4", "sp44")
            .setFreeSpaceMgrName("shared4")
            .build();

        StorPool sp54 = storPoolTestFactory.builder("node5", "sp54")
            .setFreeSpaceMgrName("shared4")
            .build();

        Resource rsc1 = volumeTestFactory.builder("node1", "rsc1")
            .setStorPoolData(sp11)
            .build().getAbsResource();
        Resource rsc2 = volumeTestFactory.builder("node2", "rsc2")
            .setStorPoolData(sp12)
            .putStorPool(".meta", sp22)
            .build().getAbsResource();
        // no rsc3
        Node node3 = nodeTestFactory.get("node3", false);

        Resource rsc4 = volumeTestFactory.builder("node4", "rsc4")
            .setStorPoolData(sp34)
            .putStorPool(".meta", sp44)
            .build().getAbsResource();
        Resource rsc5 = volumeTestFactory.builder("node5", "rsc5")
            .setStorPoolData(sp54)
            .build().getAbsResource();

        List<StorPool> allStorPools = java.util.Arrays.asList(sp11, sp12, sp22, sp23, sp33, sp34, sp44, sp54);
        assertInactiveExcept(allStorPools);

        // rsc1 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertInactiveExcept(allStorPools, sp11);

        // rsc5 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc5));
        assertInactiveExcept(allStorPools, sp11, sp54);

        // rsc2 rejected
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertInactiveExcept(allStorPools, sp11, sp54);

        // rsc4 rejected
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc4));
        assertInactiveExcept(allStorPools, sp11, sp54);

        // node3 rejected
        assertFalse(sharedStorPoolMgr.requestSharedLock(node3));
        assertInactiveExcept(allStorPools, sp11, sp54);

        // rsc1 finished, rsc2 can start
        assertNextUpdates(
            sharedStorPoolMgr.releaseLock(sp11),
            rsc2
        );
        assertInactiveExcept(allStorPools, sp12, sp22, sp54);

        // rsc2 finished, nothing can start
        assertNextUpdates(sharedStorPoolMgr.releaseLock(sp12, sp22));
        assertInactiveExcept(allStorPools, sp54);

        // rsc5 finished, rsc4 can start
        assertNextUpdates(sharedStorPoolMgr.releaseLock(sp54), rsc4);
        assertInactiveExcept(allStorPools, sp34, sp44);

        // rsc4 finished, node3 can start
        assertNextUpdates(sharedStorPoolMgr.releaseLock(sp34, sp44), node3);
        assertInactiveExcept(allStorPools, sp23, sp33);

        // node finished, empty, done
        assertNextUpdates(sharedStorPoolMgr.releaseLock(sp23, sp33));
        assertInactiveExcept(allStorPools);
    }

    private void assertInactiveExcept(List<StorPool> allSps, StorPool... activeSps)
    {
        List<StorPool> activeSpList = Arrays.asList(activeSps);
        for (StorPool sp : allSps)
        {
            if (activeSpList.contains(sp))
            {
                assertTrue(sharedStorPoolMgr.isActive(sp));
            }
            else
            {
                assertFalse(sharedStorPoolMgr.isActive(sp));
            }
        }
    }

    private final <T> void assertNextUpdates(UpdateSet actualUS, UpdateSet expectedUS)
    {
        try
        {
            assertSetEquals(expectedUS.nodesToUpdate, actualUS.nodesToUpdate);
            assertSetEquals(expectedUS.spToUpdate, actualUS.spToUpdate);
            assertSetEquals(expectedUS.rscsToUpdate, actualUS.rscsToUpdate);
            assertSetEquals(expectedUS.snapsToUpdate, actualUS.snapsToUpdate);
        }
        catch (AssertionError err)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("Expected nodes: ").append(expectedUS.nodesToUpdate).append("\n");
            sb.append("Actual nodes:   ").append(actualUS.nodesToUpdate).append("\n");
            sb.append("Expected storage pools: ").append(expectedUS.spToUpdate).append("\n");
            sb.append("Actual storage pools:   ").append(actualUS.spToUpdate).append("\n");
            sb.append("Expected resources: ").append(expectedUS.rscsToUpdate).append("\n");
            sb.append("Actual resources:   ").append(actualUS.rscsToUpdate).append("\n");
            sb.append("Expected snapshots: ").append(expectedUS.snapsToUpdate).append("\n");
            sb.append("Actual snapshots:   ").append(actualUS.snapsToUpdate).append("\n");
            fail(sb.toString());
        }
    }

    private <T> void assertSetEquals(Set<T> expectedSet, Set<T> actualSet)
    {
        assertEquals(expectedSet.size(), actualSet.size());
        HashSet<T> copy = new HashSet<>(expectedSet);
        for (T elem : actualSet)
        {
            assertTrue(copy.remove(elem));
        }
        assertTrue(copy.isEmpty());
    }

    /*
     * Utility methods
     */
    private void assertNextUpdates(UpdateSet releaseLockRef)
    {
        assertNextUpdates(releaseLockRef, emptyUpdateSet());
    }

    private void assertNextUpdates(UpdateSet releaseLockRef, Node... nodes)
    {
        assertNextUpdates(releaseLockRef, updateSetBuilder().add(nodes).build());
    }

    private void assertNextUpdates(UpdateSet releaseLockRef, StorPool... sps)
    {
        assertNextUpdates(releaseLockRef, updateSetBuilder().add(sps).build());
    }
    private void assertNextUpdates(UpdateSet releaseLockRef, Resource... rscs)
    {
        assertNextUpdates(releaseLockRef, updateSetBuilder().add(rscs).build());
    }

    private void assertNextUpdates(UpdateSet releaseLockRef, Snapshot... snaps)
    {
        assertNextUpdates(releaseLockRef, updateSetBuilder().add(snaps).build());
    }

    private UpdateSet emptyUpdateSet()
    {
        return new UpdateSet();
    }

    private UpdateSetBuilder updateSetBuilder()
    {
        return new UpdateSetBuilder();
    }


    private class UpdateSetBuilder
    {
        private UpdateSet updateSet = new UpdateSet();

        private UpdateSetBuilder add(Resource... rscs)
        {
            return add(updateSet.rscsToUpdate, rscs);
        }

        private UpdateSetBuilder add(Node... nodes)
        {
            return add(updateSet.nodesToUpdate, nodes);
        }

        private UpdateSetBuilder add(StorPool... storPools)
        {
            return add(updateSet.spToUpdate, storPools);
        }

        private UpdateSetBuilder add(Snapshot... snaps)
        {
            return add(updateSet.snapsToUpdate, snaps);
        }

        @SuppressWarnings("unchecked")
        private <T> UpdateSetBuilder add(Set<T> set, T... elements)
        {
            set.addAll(Arrays.asList(elements));
            return this;
        }

        private UpdateSet build()
        {
            return updateSet;
        }
    }
}
