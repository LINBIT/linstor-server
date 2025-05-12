package com.linbit.linstor.core;

import com.linbit.linstor.core.identifier.SharedStorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.TestAccessContextProvider;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SharedStorPoolManagerTest extends GenericDbBase
{
    private static final long GiB = 1024 * 1024; // base-unit in KiB

    private SharedStorPoolManager sharedStorPoolMgr;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception
    {
        setUpAndEnterScope();

        sharedStorPoolMgr = new SharedStorPoolManager(
            TestAccessContextProvider.SYS_CTX,
            errorReporter
        );

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

        Volume vlm = volumeTestFactory.builder("node", "rsc").setStorPoolData(sp).build();
        Resource rsc = vlm.getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sp));

        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc)); // granted
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertNextNodes(sharedStorPoolMgr.releaseLocks(sp.getNode()));
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

        Volume vlm = volumeTestFactory.builder("node", "rsc").setStorPoolData(sp).build();
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

        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sp.getNode()),
            expectedPairsFrom(sp)
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sp.getNode())
            // empty, done, previous 2x rejects were grouped together
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

        Volume vlm = volumeTestFactory.builder("node", "rsc").setStorPoolData(sp).build();
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
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sp.getNode()),
            expectedPairsFrom(sp)
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // no more updates, done
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sp.getNode())
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
        StorPool simpleSp = storPoolTestFactory.builder("node2", "spSimple")
            .build();

        Volume sharedVlm = volumeTestFactory.builder("node", "rsc")
            .setStorPoolData(sharedSp).build();
        Resource sharedRsc = sharedVlm.getAbsResource();

        Volume simpleVlm = volumeTestFactory.builder("node2", "rsc2")
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

        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp.getNode()),
            expectedPairsFrom(sharedSp)
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp.getNode())
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

        Resource rsc1 = volumeTestFactory.builder("node", "rsc1").setStorPoolData(sharedSp).build().getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSp));

        // first come, first serve
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc1 still in progress
        Resource rsc2 = volumeTestFactory.builder("node", "rsc2").setStorPoolData(sharedSp).build().getAbsResource();
        Resource rsc3 = volumeTestFactory.builder("node", "rsc3").setStorPoolData(sharedSp).build().getAbsResource();
        Resource rsc4 = volumeTestFactory.builder("node", "rsc4").setStorPoolData(sharedSp).build().getAbsResource();

        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc3));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc4));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc1 finally finishes
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp.getNode()),
            expectedPairsFrom(sharedSp)
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc2-4 finish
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp.getNode())
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

        Resource rsc11 = volumeTestFactory.builder("n1", "rsc1").setStorPoolData(sharedSpN1).build().getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // first come, first serve
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc11));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // n1/rsc1 still in progress
        Resource rsc21 = volumeTestFactory.builder("n2", "rsc1").setStorPoolData(sharedSpN2).build().getAbsResource();
        Resource rsc12 = volumeTestFactory.builder("n1", "rsc2").setStorPoolData(sharedSpN1).build().getAbsResource();
        Resource rsc22 = volumeTestFactory.builder("n2", "rsc2").setStorPoolData(sharedSpN2).build().getAbsResource();

        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc21));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc12));
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc22));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));

        // n1/rsc1 finally finishes
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSpN1.getNode()),
            expectedPairsFrom(sharedSpN2)
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN2));

        // n2/rsc1 and n2/rsc2 done
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSpN2.getNode()),
            expectedPairsFrom(sharedSpN1)
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // all done
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSpN1.getNode())
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
        StorPool sharedSp1 = storPoolTestFactory.builder("node1", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool sharedSp2 = storPoolTestFactory.builder("node2", "spShared2")
            .setFreeSpaceMgrName("shared2")
            .build();

        StorPool sharedSp31 = storPoolTestFactory.builder("node3", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool sharedSp32 = storPoolTestFactory.builder("node3", "spShared2")
            .setFreeSpaceMgrName("shared2")
            .build();

        // in spShared
        Resource rsc1 = volumeTestFactory.builder("node1", "rsc1")
            .setStorPoolData(sharedSp1)
            .build().getAbsResource();
        // in spShared2
        Resource rsc2 = volumeTestFactory.builder("node2", "rsc2")
            .setStorPoolData(sharedSp2)
            .build().getAbsResource();
        // in spShared1 AND spShared2
        Resource rsc12 = volumeTestFactory.builder("node3", "rsc12")
            .setStorPoolData(sharedSp31)
            .putStorPool(".meta", sharedSp32)
            .build().getAbsResource();

        List<StorPool> allSps = Arrays.asList(sharedSp1, sharedSp2, sharedSp31, sharedSp32);

        assertInactiveExcept(allSps);

        // rsc1 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertInactiveExcept(allSps, sharedSp1);

        // rsc2 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertInactiveExcept(allSps, sharedSp1, sharedSp2);

        // rsc12 has to be delayed
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc12));
        assertInactiveExcept(allSps, sharedSp1, sharedSp2);

        // rsc2 finishes, rsc12 still delayed
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp2.getNode())
            // empty
        );
        assertInactiveExcept(allSps, sharedSp1);

        // rsc1 finishes, rsc12 can start
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp1.getNode()),
            expectedPairsFrom(sharedSp31, sharedSp32)
        );
        assertInactiveExcept(allSps, sharedSp31, sharedSp32);

        // rsc12 finishes, all done
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp31.getNode())
        );
        assertInactiveExcept(allSps);
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
        StorPool sharedSp2 = storPoolTestFactory.builder("node2", "spShared2")
            .setFreeSpaceMgrName("shared2")
            .build();

        StorPool sharedSp13 = storPoolTestFactory.builder("node3", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool sharedSp23 = storPoolTestFactory.builder("node3", "spShared2")
            .setFreeSpaceMgrName("shared2")
            .build();

        // in spShared
        Resource rsc1 = volumeTestFactory.builder("node", "rsc1")
            .setStorPoolData(sharedSp1)
            .build().getAbsResource();
        // in spShared2
        Resource rsc2 = volumeTestFactory.builder("node2", "rsc2")
            .setStorPoolData(sharedSp2)
            .build().getAbsResource();
        // in spShared1 AND spShared2
        Resource rsc12 = volumeTestFactory.builder("node3", "rsc12")
            .setStorPoolData(sharedSp13)
            .putStorPool(".meta", sharedSp23)
            .build().getAbsResource();

        List<StorPool> allSps = Arrays.asList(sharedSp1, sharedSp2, sharedSp13, sharedSp23);
        assertInactiveExcept(allSps);

        // rsc1 can start
        assertTrue(sharedStorPoolMgr.requestSharedLock(rsc1));
        assertInactiveExcept(allSps, sharedSp1);

        // rsc12 gets rejected
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc12));
        assertInactiveExcept(allSps, sharedSp1);

        // rsc2 rejected
        assertFalse(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertInactiveExcept(allSps, sharedSp1);

        // rsc1 finishes, rsc12 can go
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp1.getNode()),
            expectedPairsFrom(sharedSp13, sharedSp23)
        );
        assertInactiveExcept(allSps, sharedSp13, sharedSp23);

        // rsc12 finishes, rsc2 can start
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp13.getNode()),
            expectedPairsFrom(sharedSp2)
        );
        assertInactiveExcept(allSps, sharedSp2);

        // rsc2 finishes, all done
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp2.getNode())
        );
        assertInactiveExcept(allSps);
    }

    @Test
    public void releaseUnlockedSp() throws Exception
    {
        StorPool sharedSp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .build();
        StorPool simpleSp = storPoolTestFactory.builder("node", "sp1")
            .build();

        // not locked, nothing to release
        sharedStorPoolMgr.releaseLocks(sharedSp.getNode());
        sharedStorPoolMgr.releaseLocks(simpleSp.getNode()); // no error
    }

    @Test()
    public void unmanagedSharedSp() throws Exception
    {
        StorPool extLockSharedSp = storPoolTestFactory.builder("node", "spShared")
            .setFreeSpaceMgrName("shared")
            .setExternalLocking(true)
            .build();

        assertTrue(sharedStorPoolMgr.isActive(extLockSharedSp)); // external locking
        assertTrue(sharedStorPoolMgr.requestSharedLock(extLockSharedSp));
        assertTrue(sharedStorPoolMgr.requestSharedLock(extLockSharedSp));
        assertTrue(sharedStorPoolMgr.requestSharedLock(extLockSharedSp)); // not considered "shared"

        assertNextNodes(sharedStorPoolMgr.releaseLocks(extLockSharedSp.getNode()));
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

        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp1.getNode()),
            expectedPairsFrom(sharedSp2)
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sharedSp2.getNode())
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
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(sp11.getNode()),
            expectedPairsFrom(sp12, sp22)
        );
        assertInactiveExcept(allStorPools, sp12, sp22, sp54);

        // rsc2 finished, nothing can start
        assertNextNodes(sharedStorPoolMgr.releaseLocks(sp12.getNode()));
        assertInactiveExcept(allStorPools, sp54);

        // rsc5 finished, rsc4 can start
        assertNextNodes(sharedStorPoolMgr.releaseLocks(sp54.getNode()), expectedPairsFrom(sp34, sp44));
        assertInactiveExcept(allStorPools, sp34, sp44);

        // rsc4 finished, node3 can start
        assertNextNodes(sharedStorPoolMgr.releaseLocks(sp34.getNode()), expectedPairsFrom(sp23, sp33));
        assertInactiveExcept(allStorPools, sp23, sp33);

        // node finished, empty, done
        assertNextNodes(sharedStorPoolMgr.releaseLocks(sp23.getNode()));
        assertInactiveExcept(allStorPools);
    }

    /*
     * This testcase recreates a bug where the second node requested 1 shared lock but received 2.
     */
    @Test
    public void multipleSharedPoolsTest() throws Exception
    {
        StorPool sp11 = storPoolTestFactory.builder("node1", "sp1")
            .setFreeSpaceMgrName("shared1")
            .build();
        StorPool sp21 = storPoolTestFactory.builder("node2", "sp1")
            .setFreeSpaceMgrName("shared1")
            .build();
        StorPool sp12 = storPoolTestFactory.builder("node1", "sp2")
            .setFreeSpaceMgrName("shared2")
            .build();
        StorPool sp22 = storPoolTestFactory.builder("node2", "sp2")
            .setFreeSpaceMgrName("shared2")
            .build();

        Resource rsc11 = volumeTestFactory.builder("node1", "rsc1")
            .setStorPoolData(sp11)
            .build().getAbsResource();
        Resource rsc12 = volumeTestFactory.builder("node1", "rsc2")
            .setStorPoolData(sp11)
            .build().getAbsResource();
        Resource rsc21 = volumeTestFactory.builder("node2", "rsc1")
            .setStorPoolData(sp21)
            .build().getAbsResource();
        Resource rsc22 = volumeTestFactory.builder("node2", "rsc2")
            .setStorPoolData(sp22)
            .build().getAbsResource();

        Node node1 = sp11.getNode();
        Node node2 = sp21.getNode();

        SharedStorPoolName sharedName1 = sp11.getSharedStorPoolName();
        SharedStorPoolName sharedName2 = sp12.getSharedStorPoolName();

        List<StorPool> allStorPools = java.util.Arrays.asList(sp11, sp12, sp21, sp22);
        assertInactiveExcept(allStorPools);

        // rsc11 can start
        assertTrue(sharedStorPoolMgr.requestSharedLocks(node1, Arrays.asList(sharedName1)));
        assertInactiveExcept(allStorPools, sp11);

        // rsc11 finished, nothing to start
        assertNextNodes(sharedStorPoolMgr.releaseLocks(node1));
        assertInactiveExcept(allStorPools);

        // rsc11 needs to be run again
        assertTrue(sharedStorPoolMgr.requestSharedLocks(node1, Arrays.asList(sharedName1)));
        assertInactiveExcept(allStorPools, sp11);

        // rsc21 starts, but is blocked
        assertFalse(sharedStorPoolMgr.requestSharedLocks(node2, Arrays.asList(sharedName1)));
        assertInactiveExcept(allStorPools, sp11);

        // rsc11 finishes, rsc21 can start
        assertNextNodes(
            sharedStorPoolMgr.releaseLocks(node1),
            new ExpectedPair(
                node2,
                sharedName1 // bug: node2 receives also sharedName2 lock, which was never requested
            )
        );
        assertInactiveExcept(allStorPools, sp21);
    }

    private void assertInactiveExcept(List<StorPool> allSps, StorPool... activeSps)
    {
        List<StorPool> activeSpList = Arrays.asList(activeSps);
        StringBuilder sb = new StringBuilder("\n");
        boolean allCorrect = true;
        for (StorPool sp : allSps)
        {
            sb.append(sp);
            boolean isActive = sharedStorPoolMgr.isActive(sp);
            boolean correct = activeSpList.contains(sp) == isActive;
            sb.append("  ");
            if (correct)
            {
                if (!isActive)
                {
                    sb.append("in");
                }
                sb.append("active (correct)\n");
            }
            else
            {
                if (!isActive)
                {
                    sb.append("in");
                }
                sb.append("active but should not be\n");
                allCorrect = false;
            }
        }
        if (!allCorrect)
        {
            fail(sb.toString());
        }
    }

    private ExpectedPair[] expectedPairsFrom(StorPool... storPools)
    {
        Map<Node, Set<SharedStorPoolName>> expectedMap = new TreeMap<>();
        for (StorPool sp : storPools)
        {
            expectedMap.computeIfAbsent(sp.getNode(), ignore -> new TreeSet<>()).add(sp.getSharedStorPoolName());
        }

        ExpectedPair[] ret = new ExpectedPair[expectedMap.size()];
        int idx = 0;
        for (Entry<Node, Set<SharedStorPoolName>> entry : expectedMap.entrySet())
        {
            ret[idx] = new ExpectedPair(entry.getKey(), entry.getValue());
            idx++;
        }
        return ret;
    }

    private void assertNextNodes(Map<Node, Set<SharedStorPoolName>> actualNext, ExpectedPair... expectedNext)
    {
        assertEquals(expectedNext.length, actualNext.size());

        for (ExpectedPair pair : expectedNext)
        {
            Set<SharedStorPoolName> actualSspNameSet = actualNext.get(pair.node);
            assertNotNull(actualSspNameSet);
            assertSetEquals(pair.sspNameSet, actualSspNameSet);
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

    private static class ExpectedPair
    {
        private Node node;
        private Set<SharedStorPoolName> sspNameSet;

        public ExpectedPair(Node nodeRef, SharedStorPoolName... sspNames)
        {
            this(nodeRef, new HashSet<>(Arrays.asList(sspNames)));
        }

        public ExpectedPair(Node nodeRef, Set<SharedStorPoolName> sspNameSetRef)
        {
            node = nodeRef;
            sspNameSet = sspNameSetRef;
        }
    }
}
