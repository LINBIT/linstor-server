package com.linbit.linstor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.SatelliteTransactionMgr;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.ResourceDefinition.TransportType;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.dbdrivers.SatelliteDbModule;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DummySecurityInitializer;
import com.linbit.linstor.security.TestSecurityModule;
import com.linbit.linstor.testutils.TestCoreModule;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourceDefinitionDataSatelliteTest
{
    private static final AccessContext SYS_CTX = DummySecurityInitializer.getSystemAccessContext();

    private final ResourceName resName;
    private final TcpPortNumber port;
    private final TransportType transportType;

    private java.util.UUID resDfnUuid;

    @Inject private VolumeDefinitionDataSatelliteFactory volumeDefinitionDataFactory;
    @Inject private ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactory;

    public ResourceDefinitionDataSatelliteTest() throws InvalidNameException, ValueOutOfRangeException
    {
        resName = new ResourceName("TestResName");
        port = new TcpPortNumber(4242);
        transportType = TransportType.IP;
    }

    @Before
    public void setUp() throws Exception
    {
        Injector injector = Guice.createInjector(
            new GuiceConfigModule(),
            new LoggingModule(new StdErrorReporter("TESTS", "")),
            new TestSecurityModule(SYS_CTX),
            new CoreModule(),
            new TestCoreModule(),
            new SatelliteDbModule()
        );
        injector.injectMembers(this);

        resDfnUuid = UUID.randomUUID();
    }

    @Test
    public void testDirtyParent() throws Exception
    {
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transportType,
            transMgr
        );
        rscDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");

        VolumeDefinitionData vlmDfn = volumeDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            java.util.UUID.randomUUID(),
            rscDfn,
            new VolumeNumber(0),
            1000,
            new MinorNumber(10),
            null,
            transMgr
        );
        vlmDfn.setConnection(transMgr);

    }

    @Test (expected = ImplementationError.class)
    /**
     * Check that an active transaction on an object can't be replaced
     */
    public void testReplaceActiveTransaction() throws Exception
    {
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transportType,
            transMgr
        );
        SatelliteTransactionMgr transMgrOther = new SatelliteTransactionMgr();
        rscDfn.setConnection(transMgrOther); // throws ImplementationError
        rscDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");
    }

    @Test
    /**
     * This test checks that a new transaction can be set after a commit.
     */
    public void testNewTransaction() throws Exception
    {
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transportType,
            transMgr
        );
        transMgr.commit();

        assertEquals(0, transMgr.sizeObjects());
        assertFalse(rscDfn.hasTransMgr());

        SatelliteTransactionMgr transMgrOther = new SatelliteTransactionMgr();
        rscDfn.setConnection(transMgrOther);
        rscDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");
        assertTrue(rscDfn.hasTransMgr());
        assertTrue(rscDfn.isDirty());
    }

    @Test (expected = ImplementationError.class)
    /**
     * This test should fail because the resourcedef properties are changed without an active transaction.
     */
    public void testDirtyObject() throws Exception
    {
        SatelliteTransactionMgr transMgr = new SatelliteTransactionMgr();
        ResourceDefinitionData rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resName,
            port,
            null,
            "notTellingYou",
            transportType,
            transMgr
        );
        transMgr.commit();

        rscDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");
        SatelliteTransactionMgr transMgrOther = new SatelliteTransactionMgr();
        rscDfn.setConnection(transMgrOther); // throws ImplementationError
    }
}
