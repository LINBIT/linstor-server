package com.linbit.linstor;

import com.linbit.GuiceConfigModule;
import com.linbit.ImplementationError;
import com.linbit.InvalidNameException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.api.LinStorScope;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.core.identifier.ResourceGroupName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.core.identifier.VolumeNumber;
import com.linbit.linstor.core.objects.ResourceDefinition.TransportType;
import com.linbit.linstor.core.objects.ResourceDefinitionData;
import com.linbit.linstor.core.objects.ResourceDefinitionDataSatelliteFactory;
import com.linbit.linstor.core.objects.ResourceGroupDataSatelliteFactory;
import com.linbit.linstor.core.objects.VolumeDefinitionData;
import com.linbit.linstor.core.objects.VolumeDefinitionDataSatelliteFactory;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.SatelliteDbModule;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DummySecurityInitializer;
import com.linbit.linstor.security.TestApiModule;
import com.linbit.linstor.security.TestSecurityModule;
import com.linbit.linstor.transaction.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.SatelliteTransactionMgrModule;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.nio.file.Paths;
import java.util.UUID;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ResourceDefinitionDataSatelliteTest
{
    private static final AccessContext SYS_CTX = DummySecurityInitializer.getSystemAccessContext();

    private final ResourceName resName;
    private final TcpPortNumber port;
    private final TransportType transportType;

    private java.util.UUID resDfnUuid;

    @Inject private VolumeDefinitionDataSatelliteFactory volumeDefinitionDataFactory;
    @Inject private ResourceDefinitionDataSatelliteFactory resourceDefinitionDataFactory;
    @Inject private ResourceGroupDataSatelliteFactory resourceGroupDataFactory;

    @Inject private LinStorScope testScope;
    @Inject private Provider<TransactionMgr> transMgrProvider;

    @SuppressWarnings("checkstyle:magicnumber")
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
            new LoggingModule(new StdErrorReporter("TESTS", Paths.get("build/test-logs"), true, "", null, () -> null)),
            new TestSecurityModule(SYS_CTX),
            new CoreModule(),
            new SatelliteDbModule(),
            new SatelliteTransactionMgrModule(),
            new TestApiModule()
        );
        injector.injectMembers(this);
        TransactionMgr transMgr = new SatelliteTransactionMgr();
        testScope.enter();
        testScope.seed(TransactionMgr.class, transMgr);

        resDfnUuid = UUID.randomUUID();
    }

    @After
    public void tearDown() throws Exception
    {
        testScope.exit();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testDirtyParent() throws Exception
    {
        ResourceDefinitionData rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resourceGroupDataFactory.getInstanceSatellite(
                UUID.randomUUID(),
                new ResourceGroupName(InternalApiConsts.DEFAULT_RSC_GRP_NAME),
                "",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            resName,
            null
        );
        rscDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");

        VolumeDefinitionData vlmDfn = volumeDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            java.util.UUID.randomUUID(),
            rscDfn,
            new VolumeNumber(0),
            1000,
            null
        );
    }

    @Test (expected = ImplementationError.class)
    /**
     * Check that an active transaction on an object can't be replaced
     */
    public void testReplaceActiveTransaction() throws Exception
    {
        ResourceDefinitionData rscDfn = resourceDefinitionDataFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resourceGroupDataFactory.getInstanceSatellite(
                UUID.randomUUID(),
                new ResourceGroupName(InternalApiConsts.DEFAULT_RSC_GRP_NAME),
                "",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            resName,
            null
        );
        rscDfn.getProps(SYS_CTX).setProp("test", "make this rscDfn dirty");
        // do not commit

        SatelliteTransactionMgr transMgrOther = new SatelliteTransactionMgr();
        rscDfn.getProps(SYS_CTX).setConnection(transMgrOther); // throws ImplementationError
    }
}
