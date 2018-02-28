package com.linbit.linstor.numberpool;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.ImplementationError;
import com.linbit.ValueInUseException;
import com.linbit.ValueOutOfRangeException;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.ResourceDefinition;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.VolumeDefinition;
import com.linbit.linstor.annotation.SystemContext;
import com.linbit.linstor.annotation.Uninitialized;
import com.linbit.linstor.core.ControllerCoreModule;
import com.linbit.linstor.core.CoreModule;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Iterator;

public class NumberPoolModule extends AbstractModule
{
    public static final String MINOR_NUMBER_POOL = "MinorNumberPool";
    public static final String UNINITIALIZED_MINOR_NUMBER_POOL = "UninitializedMinorNumberPool";
    public static final String TCP_PORT_POOL = "TcpPortPool";
    public static final String UNINITIALIZED_TCP_PORT_POOL = "UninitializedTcpPortPool";

    private static final String PROPSCON_KEY_MINOR_NR_RANGE = "minorNrRange";
    private static final String MINOR_NR_ELEMENT_NAME = "Minor number";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_MINOR_NR_MIN = 1000;
    private static final int DEFAULT_MINOR_NR_MAX = 49999;

    private static final String PROPSCON_KEY_TCP_PORT_RANGE = "tcpPortRange";
    private static final String TCP_PORT_ELEMENT_NAME = "TCP port";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_TCP_PORT_MIN = 7000;
    private static final int DEFAULT_TCP_PORT_MAX = 7999;

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    @Named(UNINITIALIZED_MINOR_NUMBER_POOL)
    public DynamicNumberPool minorNrPool(
        ErrorReporter errorReporter,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props ctrlConfRef
    )
    {
        DynamicNumberPool minorNrPool = new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            PROPSCON_KEY_MINOR_NR_RANGE,
            MINOR_NR_ELEMENT_NAME,
            MinorNumber::minorNrCheck,
            MinorNumber.MINOR_NR_MAX,
            DEFAULT_MINOR_NR_MIN,
            DEFAULT_MINOR_NR_MAX
        );

        minorNrPool.reloadRange();

        return minorNrPool;
    }

    @Provides
    @Singleton
    @Named(MINOR_NUMBER_POOL)
    public DynamicNumberPool initializeMinorNrPool(
        ErrorReporter errorReporter,
        @SystemContext AccessContext initCtx,
        @Named(UNINITIALIZED_MINOR_NUMBER_POOL) DynamicNumberPool minorNrPool,
        CoreModule.ResourceDefinitionMap rscDfnMap
    )
    {
        try
        {
            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                Iterator<VolumeDefinition> vlmIter = curRscDfn.iterateVolumeDfn(initCtx);
                while (vlmIter.hasNext())
                {
                    VolumeDefinition curVlmDfn = vlmIter.next();
                    MinorNumber minorNr = curVlmDfn.getMinorNr(initCtx);
                    try
                    {
                        minorNrPool.allocate(minorNr.value);
                    }
                    catch (ValueOutOfRangeException | ValueInUseException exc)
                    {
                        errorReporter.logError(
                            "Skipping initial allocation in pool: " + exc.getMessage());
                    }
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "An " + accExc.getClass().getSimpleName() + " exception was generated " +
                    "during number allocation cache initialization",
                accExc
            );
        }

        return minorNrPool;
    }

    @Provides
    @Singleton
    @Named(UNINITIALIZED_TCP_PORT_POOL)
    public DynamicNumberPool tcpPortPool(
        ErrorReporter errorReporter,
        @Named(ControllerCoreModule.CONTROLLER_PROPS) Props ctrlConfRef
    )
    {
        DynamicNumberPool tcpPortPool = new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            PROPSCON_KEY_TCP_PORT_RANGE,
            TCP_PORT_ELEMENT_NAME,
            TcpPortNumber::tcpPortNrCheck,
            TcpPortNumber.PORT_NR_MAX,
            DEFAULT_TCP_PORT_MIN,
            DEFAULT_TCP_PORT_MAX
        );

        tcpPortPool.reloadRange();

        return tcpPortPool;
    }

    @Provides
    @Singleton
    @Named(TCP_PORT_POOL)
    public DynamicNumberPool initializeTcpPortPool(
        ErrorReporter errorReporter,
        @SystemContext AccessContext initCtx,
        @Named(UNINITIALIZED_TCP_PORT_POOL) DynamicNumberPool tcpPortPool,
        CoreModule.ResourceDefinitionMap rscDfnMap
    )
    {
        try
        {
            for (ResourceDefinition curRscDfn : rscDfnMap.values())
            {
                TcpPortNumber portNr = curRscDfn.getPort(initCtx);
                try
                {
                    tcpPortPool.allocate(portNr.value);
                }
                catch (ValueOutOfRangeException | ValueInUseException exc)
                {
                    errorReporter.logError(
                        "Skipping initial allocation in pool: " + exc.getMessage());
                }
            }
        }
        catch (AccessDeniedException accExc)
        {
            throw new ImplementationError(
                "An " + accExc.getClass().getSimpleName() + " exception was generated " +
                    "during number allocation cache initialization",
                accExc
            );
        }

        return tcpPortPool;
    }
}
