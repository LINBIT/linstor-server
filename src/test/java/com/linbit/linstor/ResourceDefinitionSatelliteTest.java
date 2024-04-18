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
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.core.objects.ResourceDefinitionSatelliteFactory;
import com.linbit.linstor.core.objects.ResourceGroupSatelliteFactory;
import com.linbit.linstor.core.objects.VolumeDefinition;
import com.linbit.linstor.core.objects.VolumeDefinitionSatelliteFactory;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.dbdrivers.SatelliteDbModule;
import com.linbit.linstor.logging.LoggingModule;
import com.linbit.linstor.logging.StdErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DummySecurityInitializer;
import com.linbit.linstor.security.TestApiModule;
import com.linbit.linstor.security.TestSecurityModule;
import com.linbit.linstor.storage.interfaces.layers.drbd.DrbdRscDfnObject.TransportType;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgr;
import com.linbit.linstor.transaction.manager.SatelliteTransactionMgrModule;
import com.linbit.linstor.transaction.manager.TransactionMgr;

import javax.inject.Inject;
import javax.inject.Provider;

import java.nio.file.Paths;
import java.util.UUID;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ResourceDefinitionSatelliteTest
{
    private static final AccessContext SYS_CTX = DummySecurityInitializer.getSystemAccessContext();

    private final ResourceName resName;
    private final TcpPortNumber port;
    private final TransportType transportType;

    private java.util.UUID resDfnUuid;
    LinStorScope.ScopeAutoCloseable close;

    @Inject
    private VolumeDefinitionSatelliteFactory volumeDefinitionFactory;
    @Inject
    private ResourceDefinitionSatelliteFactory resourceDefinitionFactory;
    @Inject
    private ResourceGroupSatelliteFactory resourceGroupFactory;

    @Inject
    private LinStorScope testScope;
    @Inject
    private Provider<TransactionMgr> transMgrProvider;

    private StdErrorReporter errorReporter;

    @SuppressWarnings("checkstyle:magicnumber")
    public ResourceDefinitionSatelliteTest() throws InvalidNameException, ValueOutOfRangeException
    {
        resName = new ResourceName("TestResName");
        port = new TcpPortNumber(4242);
        transportType = TransportType.IP;
    }

    @Before
    public void setUp() throws Exception
    {
        errorReporter = new StdErrorReporter("TESTS", Paths.get("build/test-logs"), true, "", null, null, () -> null);
        Injector injector = Guice.createInjector(
            new GuiceConfigModule(),
            new LoggingModule(errorReporter),
            new TestSecurityModule(SYS_CTX),
            new CoreModule(),
            new SatelliteDbModule(),
            new SatelliteTransactionMgrModule(),
            new TestApiModule()
        );
        injector.injectMembers(this);
        TransactionMgr transMgr = new SatelliteTransactionMgr();
        // do not use scopes like this unless you absolutely need it for a test, use try-with-resource whenever possible
        close = testScope.enter();
        testScope.seed(TransactionMgr.class, transMgr);

        resDfnUuid = UUID.randomUUID();
    }

    @After
    public void tearDown() throws Exception
    {
        close.close();
        errorReporter.shutdown();
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Test
    public void testDirtyParent() throws Exception
    {
        ResourceDefinition rscDfn = resourceDefinitionFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resourceGroupFactory.getInstanceSatellite(
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

        VolumeDefinition vlmDfn = volumeDefinitionFactory.getInstanceSatellite(
            SYS_CTX,
            java.util.UUID.randomUUID(),
            rscDfn,
            new VolumeNumber(0),
            1000,
            null
        );
    }

    @Test(expected = ImplementationError.class)
    /**
     * Check that an active transaction on an object can't be replaced
     */
    public void testReplaceActiveTransaction() throws Exception
    {
        ResourceDefinition rscDfn = resourceDefinitionFactory.getInstanceSatellite(
            SYS_CTX,
            resDfnUuid,
            resourceGroupFactory.getInstanceSatellite(
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
