package com.linbit.drbdmanage.drbdstate;

import com.linbit.ValueOutOfRangeException;
import com.linbit.drbdmanage.MinorNumber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_CONN;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_CONN_CRT;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_CONN_DSTR;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_DISK;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_MINOR;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_PEER_ROLE;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_REPL;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_RES_CRT;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_RES_DSTR;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_ROLE;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_VOL_CRT;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_VOL_DSTR;
import static com.linbit.drbdmanage.drbdstate.StateTracker.OBS_ALL;

/**
 * Test of the DRBD resource observers
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class StateTrackerTest
{
    private StateTracker tracker;
    private StateTracker.ResObsMux mux;
    private TestResObs resObs;

    public StateTrackerTest()
    {
    }

    @Before
    public void setUp()
    {
        tracker = new StateTracker();
        mux = new StateTracker.ResObsMux(tracker);
        resObs = new TestResObs();
    }

    @After
    public void tearDown()
    {
    }

    /**
     * Test of addObserver method, of class StateTracker.
     */
    @Test
    public void testObserveResCreate() throws ValueOutOfRangeException
    {
        resObs.expect(StateTracker.OBS_RES_CRT);
        tracker.addObserver(resObs, OBS_RES_CRT);

        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource instance?
        mux.resourceCreated(null);
        resObs.assertTriggered();

        // Should not trigger the ResourceObserver
        mux.minorNrChanged(null, null, new MinorNumber(5), new MinorNumber(10));

        tracker.removeObserver(resObs);
    }

    /**
     * Test of addObserver method, of class StateTracker.
     */
    @Test
    public void testObserveResDestroy()
    {
        resObs.expect(StateTracker.OBS_RES_DSTR);
        tracker.addObserver(resObs, OBS_RES_DSTR);

        // Should not trigger the ResourceObserver
        mux.resourceCreated(null);

        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource instance?
        mux.resourceDestroyed(null);
        resObs.assertTriggered();

        tracker.removeObserver(resObs);
    }

    @Test
    public void testObserveVolEvents() throws ValueOutOfRangeException
    {
        tracker.addObserver(
            resObs,
            OBS_VOL_CRT | OBS_VOL_DSTR | OBS_MINOR | OBS_DISK | OBS_REPL
        );

        resObs.expect(StateTracker.OBS_MINOR);
        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource, DrbdVolume instances?
        mux.minorNrChanged(null, null, new MinorNumber(5), new MinorNumber(10));
        resObs.assertTriggered();
        resObs.reset();

        // Should not trigger the ResourceObserver
        mux.resourceCreated(null);

        resObs.expect(StateTracker.OBS_REPL);
        // Should trigger the ResourceObserver
        // FIXME: dummy DrbdResource, DrbdVolume, DrbdConnection instances?
        mux.replicationStateChanged(null, null, null, DrbdVolume.ReplState.OFF, DrbdVolume.ReplState.SYNC_SOURCE);
        resObs.assertTriggered();

        tracker.removeObserver(resObs);

        resObs.expect(-1);
        mux.minorNrChanged(null, null, new MinorNumber(10), new MinorNumber(20));
    }

    @Test
    public void testObserveConnEvents()
    {
        tracker.addObserver(resObs, OBS_CONN | OBS_CONN_CRT | OBS_CONN_DSTR);

        // Should not trigger the ResourceObserver
        mux.roleChanged(null, DrbdResource.Role.PRIMARY, DrbdResource.Role.SECONDARY);


        resObs.expect(OBS_CONN_CRT);
        // Should trigger the ResourceObserver
        mux.connectionCreated(null, null);
        resObs.assertTriggered();

        resObs.expect(OBS_CONN);
        // Should trigger the ResourceObserver
        mux.connectionStateChanged(
            null, null,
            DrbdConnection.State.CONNECTED, DrbdConnection.State.PROTOCOL_ERROR
        );
        resObs.assertTriggered();

        resObs.expect(OBS_CONN_DSTR);
        // Should trigger the ResourceObserver
        mux.connectionDestroyed(null, null);
        resObs.assertTriggered();

        tracker.removeObserver(resObs);
    }

    @Test
    public void testObserveAllEvents()
    {
        tracker.addObserver(resObs, OBS_ALL);

        resObs.expect(OBS_ROLE);
        mux.roleChanged(null, DrbdResource.Role.UNKNOWN, DrbdResource.Role.PRIMARY);
        resObs.assertTriggered();

        resObs.expect(OBS_PEER_ROLE);
        mux.peerRoleChanged(null, null, DrbdResource.Role.PRIMARY, DrbdResource.Role.UNKNOWN);
        resObs.assertTriggered();

        resObs.expect(OBS_RES_DSTR);
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
            if (eventId == OBS_RES_CRT)
            {
                    label = "OBS_RES_CRT";
            }
            else
            if (eventId == OBS_RES_DSTR)
            {
                    label = "OBS_RES_DSTR";
            }
            else
            if (eventId == OBS_ROLE)
            {
                    label = "OBS_ROLE";
            }
            else
            if (eventId == OBS_PEER_ROLE)
            {
                    label = "OBS_PEER_ROLE";
            }
            else
            if (eventId == OBS_VOL_CRT)
            {
                    label = "OBS_VOL_CRT";
            }
            else
            if (eventId == OBS_VOL_DSTR)
            {
                    label = "OBS_VOL_DSTR";
            }
            else
            if (eventId == OBS_MINOR)
            {
                    label = "OBS_MINOR";
            }
            else
            if (eventId == OBS_DISK)
            {
                    label = "OBS_DISK";
            }
            else
            if (eventId == OBS_REPL)
            {
                    label = "OBS_REPL";
            }
            else
            if (eventId == OBS_CONN_CRT)
            {
                    label = "OBS_CONN_CRT";
            }
            else
            if (eventId == OBS_CONN_DSTR)
            {
                    label = "OBS_CONN_DSTR";
            }
            else
            if (eventId == OBS_CONN)
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
            checkExpected(StateTracker.OBS_RES_CRT);
        }

        @Override
        public void roleChanged(DrbdResource resource, DrbdResource.Role previous, DrbdResource.Role current)
        {
            checkExpected(StateTracker.OBS_ROLE);
        }

        @Override
        public void peerRoleChanged(
            DrbdResource resource, DrbdConnection connection,
            DrbdResource.Role previous, DrbdResource.Role current
        )
        {
            checkExpected(StateTracker.OBS_PEER_ROLE);
        }

        @Override
        public void resourceDestroyed(DrbdResource resource)
        {
            checkExpected(StateTracker.OBS_RES_DSTR);
        }

        @Override
        public void volumeCreated(DrbdResource resource, DrbdConnection connection, DrbdVolume volume)
        {
            checkExpected(StateTracker.OBS_VOL_CRT);
        }

        @Override
        public void minorNrChanged(
            DrbdResource resource, DrbdVolume volume,
            MinorNumber previous, MinorNumber current
        )
        {
            checkExpected(StateTracker.OBS_MINOR);
        }

        @Override
        public void diskStateChanged(
            DrbdResource resource, DrbdConnection connection, DrbdVolume volume,
            DrbdVolume.DiskState previous, DrbdVolume.DiskState current
        )
        {
            checkExpected(StateTracker.OBS_DISK);
        }

        @Override
        public void replicationStateChanged(
            DrbdResource resource, DrbdConnection connection, DrbdVolume volume,
            DrbdVolume.ReplState previous, DrbdVolume.ReplState current
        )
        {
            checkExpected(StateTracker.OBS_REPL);
        }

        @Override
        public void volumeDestroyed(DrbdResource resource, DrbdVolume volume)
        {
            checkExpected(StateTracker.OBS_VOL_DSTR);
        }

        @Override
        public void connectionCreated(DrbdResource resource, DrbdConnection connection)
        {
            checkExpected(StateTracker.OBS_CONN_CRT);
        }

        @Override
        public void connectionStateChanged(
            DrbdResource resource, DrbdConnection connection,
            DrbdConnection.State previous, DrbdConnection.State current
        )
        {
            checkExpected(StateTracker.OBS_CONN);
        }

        @Override
        public void connectionDestroyed(DrbdResource resource, DrbdConnection connection)
        {
            checkExpected(StateTracker.OBS_CONN_DSTR);
        }
    }
}
