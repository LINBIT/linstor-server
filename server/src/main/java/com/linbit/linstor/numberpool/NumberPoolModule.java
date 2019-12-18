package com.linbit.linstor.numberpool;

import com.linbit.Checks;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;

import javax.inject.Named;
import javax.inject.Singleton;

import java.util.regex.Pattern;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class NumberPoolModule extends AbstractModule
{
    public static final Pattern RANGE_PATTERN = Pattern.compile("(?<min>\\d+) ?- ?(?<max>\\d+)");

    public static final String MINOR_NUMBER_POOL = "MinorNumberPool";
    public static final String TCP_PORT_POOL = "TcpPortPool";
    public static final String LAYER_RSC_ID_POOL = "LayerRscIdPool";

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

    private static final int LAYER_RSC_ID_MIN = 0;
    private static final int LAYER_RSC_ID_MAX = BitmapPool.MAX_CAPACITY - 1;
    private static final String LAYER_RSC_ID_ELEMENT_NAME = "Layer Resource Id";

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
        return new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            ApiConsts.KEY_MINOR_NR_AUTO_RANGE,
            MINOR_NR_ELEMENT_NAME,
            MinorNumber::minorNrCheck,
            MinorNumber.MINOR_NR_MAX,
            DEFAULT_MINOR_NR_MIN,
            DEFAULT_MINOR_NR_MAX
        );
    }

    @Provides
    @Singleton
    @Named(TCP_PORT_POOL)
    public DynamicNumberPool tcpPortPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef
    )
    {
        return new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            ApiConsts.KEY_TCP_PORT_AUTO_RANGE,
            TCP_PORT_ELEMENT_NAME,
            TcpPortNumber::tcpPortNrCheck,
            TcpPortNumber.PORT_NR_MAX,
            DEFAULT_TCP_PORT_MIN,
            DEFAULT_TCP_PORT_MAX
        );
    }

    @Provides
    @Singleton
    @Named(LAYER_RSC_ID_POOL)
    public DynamicNumberPool layerRscIdPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) Props ctrlConfRef
    )
    {
        return new DynamicNumberPoolImpl(
            errorReporter,
            ctrlConfRef,
            LAYER_RSC_ID_MIN + "-" + LAYER_RSC_ID_MAX,
            LAYER_RSC_ID_ELEMENT_NAME,
            rscId -> Checks.genericRangeCheck(
                rscId,
                LAYER_RSC_ID_MIN,
                LAYER_RSC_ID_MAX,
                "Layer resource id %d is out of range [%d - %d]"
            ),
            LAYER_RSC_ID_MAX,
            LAYER_RSC_ID_MIN,
            LAYER_RSC_ID_MAX
        );
    }
}
