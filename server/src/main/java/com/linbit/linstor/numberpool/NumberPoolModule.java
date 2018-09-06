package com.linbit.linstor.numberpool;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.linbit.linstor.MinorNumber;
import com.linbit.linstor.TcpPortNumber;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.regex.Pattern;

public class NumberPoolModule extends AbstractModule
{
    public static final Pattern RANGE_PATTERN = Pattern.compile("(?<min>\\d+) ?- ?(?<max>\\d+)");

    public static final String MINOR_NUMBER_POOL = "MinorNumberPool";
    public static final String TCP_PORT_POOL = "TcpPortPool";
    public static final String SF_TARGET_PORT_POOL = "SfTargetPortPool";

    private static final String MINOR_NR_ELEMENT_NAME = "Minor number";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_MINOR_NR_MIN = 1000;
    private static final int DEFAULT_MINOR_NR_MAX = 49999;

    private static final String TCP_PORT_ELEMENT_NAME = "TCP port";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_TCP_PORT_MIN = 7000;
    private static final int DEFAULT_TCP_PORT_MAX = 7999;

    private static final int DEFAULT_SF_TARGET_TCP_PORT_MIN = 10_000;
    private static final int DEFAULT_SF_TARGET_TCP_PORT_MAX = 10_999;

    private static final String SF_TARGET_TCP_ELEMENT_NAME = "Swordfish target TCP port";

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    @Named(MINOR_NUMBER_POOL)
    public DynamicNumberPool minorNrPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef
    )
    {
        DynamicNumberPool minorNrPool = new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            ApiConsts.KEY_MINOR_NR_AUTO_RANGE,
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
    @Named(TCP_PORT_POOL)
    public DynamicNumberPool tcpPortPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef
    )
    {
        DynamicNumberPool tcpPortPool = new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            ApiConsts.KEY_TCP_PORT_AUTO_RANGE,
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
    @Named(SF_TARGET_PORT_POOL)
    public DynamicNumberPool sfTargetPortPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef
    )
    {
        DynamicNumberPool sfTargetPortPool = new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            ApiConsts.KEY_SF_TARGET_PORT_AUTO_RANGE,
            SF_TARGET_TCP_ELEMENT_NAME,
            TcpPortNumber::tcpPortNrCheck,
            TcpPortNumber.PORT_NR_MAX,
            DEFAULT_SF_TARGET_TCP_PORT_MIN,
            DEFAULT_SF_TARGET_TCP_PORT_MAX
        );

        sfTargetPortPool.reloadRange();

        return sfTargetPortPool;
    }
}
