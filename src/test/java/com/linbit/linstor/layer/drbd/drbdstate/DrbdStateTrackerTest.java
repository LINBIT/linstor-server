package com.linbit.linstor.layer.drbd.drbdstate;

import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.core.types.MinorNumber;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Test of the DRBD resource observers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class DrbdStateTrackerTest
{
    private DrbdStateTracker tracker;
    private DrbdStateTracker.ResObsMux mux;
    private TestResObs resObs;

    public DrbdStateTrackerTest()
    {
    }

    @Before
    public void setUp()
    {
        tracker = new DrbdStateTracker();
        mux = new DrbdStateTracker.ResObsMux(tracker);
        resObs = new TestResObs();
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of addObserver method, of class DrbdStateTracker.
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testObserveResCreate() throws ValueOutOfRangeException
    {
        resObs.expect(DrbdStateTracker.OBS_RES_CRT);
        tracker.addObserver(resObs, DrbdStateTracker.OBS_RES_CRT);

        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource instance?
        mux.resourceCreated(null);
        resObs.assertTriggered();

        // Should not trigger the ResourceObserver
        mux.minorNrChanged(null, null, new MinorNumber(5), new MinorNumber(10));

        tracker.removeObserver(resObs);
    }

    /**
     * Test of addObserver method, of class DrbdStateTracker.
     */
    @Test
    public void testObserveResDestroy()
    {
        resObs.expect(DrbdStateTracker.OBS_RES_DSTR);
        tracker.addObserver(resObs, DrbdStateTracker.OBS_RES_DSTR);

        // Should not trigger the ResourceObserver
        mux.resourceCreated(null);

        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource instance?
        mux.resourceDestroyed(null);
        resObs.assertTriggered();

        tracker.removeObserver(resObs);
    }

    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testObserveVolEvents() throws ValueOutOfRangeException
    {
        tracker.addObserver(
            resObs,
            DrbdStateTracker.OBS_VOL_CRT | DrbdStateTracker.OBS_VOL_DSTR |
            DrbdStateTracker.OBS_MINOR | DrbdStateTracker.OBS_DISK | DrbdStateTracker.OBS_REPL
        );

        resObs.expect(DrbdStateTracker.OBS_MINOR);
        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource, DrbdVolume instances?
        mux.minorNrChanged(null, null, new MinorNumber(5), new MinorNumber(10));
        resObs.assertTriggered();
        resObs.reset();

        // Should not trigger the ResourceObserver
        mux.resourceCreated(null);

        resObs.expect(DrbdStateTracker.OBS_REPL);
        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource, DrbdVolume, DrbdConnection instances?
        mux.replicationStateChanged(null, null, null, ReplState.OFF, ReplState.SYNC_SOURCE);
        resObs.assertTriggered();

        tracker.removeObserver(resObs);

        resObs.expect(-1);
        mux.minorNrChanged(null, null, new MinorNumber(10), new MinorNumber(20));
    }

    @Test
    public void testObserveConnEvents()
    {
        tracker.addObserver(
            resObs,
            DrbdStateTracker.OBS_CONN | DrbdStateTracker.OBS_CONN_CRT | DrbdStateTracker.OBS_CONN_DSTR
        );

        // Should not trigger the ResourceObserver
        mux.roleChanged(null, DrbdResource.Role.PRIMARY, DrbdResource.Role.SECONDARY);


        resObs.expect(DrbdStateTracker.OBS_CONN_CRT);
        // Should trigger the ResourceObserver
        mux.connectionCreated(null, null);
        resObs.assertTriggered();

        resObs.expect(DrbdStateTracker.OBS_CONN);
        // Should trigger the ResourceObserver
        mux.connectionStateChanged(
            null, null,
            DrbdConnection.State.CONNECTED, DrbdConnection.State.PROTOCOL_ERROR
        );
        resObs.assertTriggered();

        resObs.expect(DrbdStateTracker.OBS_CONN_DSTR);
        // Should trigger the ResourceObserver
        mux.connectionDestroyed(null, null);
        resObs.assertTriggered();

        tracker.removeObserver(resObs);
    }

    @Test
    public void testObserveAllEvents()
    {
        tracker.addObserver(resObs, DrbdStateTracker.OBS_ALL);

        resObs.expect(DrbdStateTracker.OBS_ROLE);
        mux.roleChanged(null, DrbdResource.Role.UNKNOWN, DrbdResource.Role.PRIMARY);
        resObs.assertTriggered();

        resObs.expect(DrbdStateTracker.OBS_PEER_ROLE);
        mux.peerRoleChanged(null, null, DrbdResource.Role.PRIMARY, DrbdResource.Role.UNKNOWN);
        resObs.assertTriggered();

        resObs.expect(DrbdStateTracker.OBS_RES_DSTR);
        mux.resourceDestroyed(null);
        resObs.assertTriggered();

        tracker.removeObserver(resObs);

        resObs.expect(-1);
        // Should not trigger the ResourceObserver
        mux.resourceCreated(null);
    }

    static class TestResObs implements ResourceObserver
    {
        private long expected;
        private boolean triggered;

        TestResObs()
        {
            expected = -1;
            triggered = false;
        }

        void expect(long eventId)
        {
            expected = eventId;
            triggered = false;
        }

        void reset()
        {
            triggered = false;
        }

        private String eventLabel(long eventId)
        {
            String label = "<NO_EVENT>";

            // Using an if-chain, because switch (variable) does not support type long
            if (eventId == DrbdStateTracker.OBS_RES_CRT)
            {
                    label = "OBS_RES_CRT";
            }
            else
            if (eventId == DrbdStateTracker.OBS_RES_DSTR)
            {
                    label = "OBS_RES_DSTR";
            }
            else
            if (eventId == DrbdStateTracker.OBS_ROLE)
            {
                    label = "OBS_ROLE";
            }
            else
            if (eventId == DrbdStateTracker.OBS_PEER_ROLE)
            {
                    label = "OBS_PEER_ROLE";
            }
            else
            if (eventId == DrbdStateTracker.OBS_VOL_CRT)
            {
                    label = "OBS_VOL_CRT";
            }
            else
            if (eventId == DrbdStateTracker.OBS_VOL_DSTR)
            {
                    label = "OBS_VOL_DSTR";
            }
            else
            if (eventId == DrbdStateTracker.OBS_MINOR)
            {
                    label = "OBS_MINOR";
            }
            else
            if (eventId == DrbdStateTracker.OBS_DISK)
            {
                    label = "OBS_DISK";
            }
            else
            if (eventId == DrbdStateTracker.OBS_REPL)
            {
                    label = "OBS_REPL";
            }
            else
            if (eventId == DrbdStateTracker.OBS_CONN_CRT)
            {
                    label = "OBS_CONN_CRT";
            }
            else
            if (eventId == DrbdStateTracker.OBS_CONN_DSTR)
            {
                    label = "OBS_CONN_DSTR";
            }
            else
            if (eventId == DrbdStateTracker.OBS_CONN)
            {
                    label = "OBS_CONN";
            }

            return label;
        }

        public void assertTriggered()
        {
            if (!triggered)
            {
                fail("ResourceObserver was not triggered as expected");
            }
        }

        private void checkExpected(long eventId)
        {
            triggered = true;
            if (expected != eventId)
            {
                String expectedStr = eventLabel(expected);
                String eventStr = eventLabel(eventId);
                fail(
                    String.format("Received unexpected event %s, expected %s", eventStr, expectedStr)
                );
            }
        }

        @Override
        public void resourceCreated(DrbdResource resource)
        {
            checkExpected(DrbdStateTracker.OBS_RES_CRT);
        }

        @Override
        public void roleChanged(DrbdResource resource, DrbdResource.Role previous, DrbdResource.Role current)
        {
            checkExpected(DrbdStateTracker.OBS_ROLE);
        }

        @Override
        public void peerRoleChanged(
            DrbdResource resource, DrbdConnection connection,
            DrbdResource.Role previous, DrbdResource.Role current
        )
        {
            checkExpected(DrbdStateTracker.OBS_PEER_ROLE);
        }

        @Override
        public void resourceDestroyed(DrbdResource resource)
        {
            checkExpected(DrbdStateTracker.OBS_RES_DSTR);
        }

        @Override
        public void volumeCreated(DrbdResource resource, DrbdConnection connection, DrbdVolume volume)
        {
            checkExpected(DrbdStateTracker.OBS_VOL_CRT);
        }

        @Override
        public void minorNrChanged(
            DrbdResource resource, DrbdVolume volume,
            MinorNumber previous, MinorNumber current
        )
        {
            checkExpected(DrbdStateTracker.OBS_MINOR);
        }

        @Override
        public void diskStateChanged(
            DrbdResource resource, DrbdConnection connection, DrbdVolume volume,
            DiskState previous, DiskState current
        )
        {
            checkExpected(DrbdStateTracker.OBS_DISK);
        }

        @Override
        public void replicationStateChanged(
            DrbdResource resource, DrbdConnection connection, DrbdVolume volume,
            ReplState previous, ReplState current
        )
        {
            checkExpected(DrbdStateTracker.OBS_REPL);
        }

        @Override
        public void volumeDestroyed(
            DrbdResource resource,
            DrbdConnection connection,
            DrbdVolume volume
        )
        {
            checkExpected(DrbdStateTracker.OBS_VOL_DSTR);
        }

        @Override
        public void connectionCreated(DrbdResource resource, DrbdConnection connection)
        {
            checkExpected(DrbdStateTracker.OBS_CONN_CRT);
        }

        @Override
        public void connectionStateChanged(
            DrbdResource resource, DrbdConnection connection,
            DrbdConnection.State previous, DrbdConnection.State current
        )
        {
            checkExpected(DrbdStateTracker.OBS_CONN);
        }

        @Override
        public void connectionDestroyed(DrbdResource resource, DrbdConnection connection)
        {
            checkExpected(DrbdStateTracker.OBS_CONN_DSTR);
        }
    }
}
