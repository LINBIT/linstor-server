package com.linbit.linstor.core;

import com.linbit.ImplementationError;
import com.linbit.linstor.core.objects.Resource;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.Volume;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.TestAccessContextProvider;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

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

        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc),
            rsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertSet(
            sharedStorPoolMgr.releaseLock(sp)
            // empty
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

        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc),
            rsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // same rsc requests same lock, but does not get it, gets queued again
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc)
            // empty, first request still in progress, lock rejected
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));
        // same rsc requests same lock, gets rejected again
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertSet(
            sharedStorPoolMgr.releaseLock(sp),
            rsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        assertSet(
            sharedStorPoolMgr.releaseLock(sp)
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

        Volume vlm = volumeTestFactory.builder("node", "rsc").build();
        Resource rsc = vlm.getAbsResource();

        assertFalse(sharedStorPoolMgr.isActive(sp));

        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc),
            rsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // first rejected attempt
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // second rejected attempt
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // update once
        assertSet(
            sharedStorPoolMgr.releaseLock(sp),
            rsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sp));

        // no more updates, done
        assertSet(
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

        assertSet(
            sharedStorPoolMgr.requestSharedLock(simpleRsc),
            simpleRsc
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // repeatable, no locks for simple storPools
        assertSet(
            sharedStorPoolMgr.requestSharedLock(simpleRsc),
            simpleRsc
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertSet(
            sharedStorPoolMgr.requestSharedLock(sharedRsc),
            sharedRsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // still no locks for simple storPools
        assertSet(
            sharedStorPoolMgr.requestSharedLock(simpleRsc),
            simpleRsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // but lock for shared SP
        assertSet(
            sharedStorPoolMgr.requestSharedLock(sharedRsc)
            // empty, first request still in progress, lock rejected
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSp),
            sharedRsc
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSp)
            // empty, done
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp));
        assertTrue(sharedStorPoolMgr.isActive(simpleSp));

        // still no locks for simple storPools
        assertSet(
            sharedStorPoolMgr.requestSharedLock(simpleRsc),
            simpleRsc
        );
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
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc1),
            rsc1
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc1 still in progress
        Resource rsc2 = volumeTestFactory.builder("node", "rsc2").build().getAbsResource();
        Resource rsc3 = volumeTestFactory.builder("node", "rsc3").build().getAbsResource();
        Resource rsc4 = volumeTestFactory.builder("node", "rsc4").build().getAbsResource();
        assertSet(sharedStorPoolMgr.requestSharedLock(rsc2));
        assertSet(sharedStorPoolMgr.requestSharedLock(rsc3));
        assertSet(sharedStorPoolMgr.requestSharedLock(rsc4));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc1 finally finishes
        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSp),
            rsc2, rsc3, rsc4
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp));

        // rsc2-4 finish
        assertSet(
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
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc11),
            rsc11
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // n1/rsc1 still in progress
        Resource rsc21 = volumeTestFactory.builder("n2", "rsc1").build().getAbsResource();
        Resource rsc12 = volumeTestFactory.builder("n1", "rsc2").build().getAbsResource();
        Resource rsc22 = volumeTestFactory.builder("n2", "rsc2").build().getAbsResource();
        assertSet(sharedStorPoolMgr.requestSharedLock(rsc21));
        assertSet(sharedStorPoolMgr.requestSharedLock(rsc12));
        assertSet(sharedStorPoolMgr.requestSharedLock(rsc22));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));

        // n1/rsc1 finally finishes
        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSpN1),
            rsc21,
            rsc22
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN2));

        // n2/rsc1 and n2/rsc2 done
        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSpN2),
            rsc12
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSpN1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSpN2));

        // all done
        assertSet(
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
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc1),
            rsc1
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc2 can start
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc2),
            rsc2
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 has to be delayed
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc12)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc2 finishes, rsc12 still delayed
        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSp2)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc1 finishes, rsc12 can start
        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSp1),
            rsc12
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 finishes, all done
        assertSet(
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
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc1),
            rsc1
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 gets rejected
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc12)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2)); // although reserved for rsc12

        // rsc2 rejected
        assertSet(
            sharedStorPoolMgr.requestSharedLock(rsc2)
            // empty
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertFalse(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc1 finishes, rsc12 can go
        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSp1),
            rsc12
        );
        assertTrue(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc12 finishes, rsc2 can start
        assertSet(
            sharedStorPoolMgr.releaseLock(sharedSp1, sharedSp2),
            rsc2
        );
        assertFalse(sharedStorPoolMgr.isActive(sharedSp1));
        assertTrue(sharedStorPoolMgr.isActive(sharedSp2));

        // rsc2 finishes, all done
        assertSet(
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

    @SafeVarargs
    private final <T> void assertSet(Set<T> set, T... elements)
    {
        try
        {
            assertEquals(elements.length, set.size());
            HashSet<T> copy = new HashSet<>(set);
            for (T elem : elements)
            {
                assertTrue(copy.remove(elem));
            }
            assertTrue(copy.isEmpty());
        }
        catch (AssertionError err)
        {
            TreeSet<String> expectedToString = new TreeSet<>();
            for (T elem : elements)
            {
                expectedToString.add(elem.toString());
            }
            TreeSet<String> actualToString = new TreeSet<>();
            for (T act : set)
            {
                actualToString.add(act.toString());
            }
            fail(
                "\n" +
                "Expected (" + elements.length + "): " + expectedToString + "\n" +
                "Actual   (" + set.size() + "): " + actualToString
            );
        }
    }
}
