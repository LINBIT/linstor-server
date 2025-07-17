package com.linbit.linstor.numberpool;

import com.linbit.Checks;
import com.linbit.linstor.PriorityProps;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.types.MinorNumber;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.ReadOnlyProps;

import javax.inject.Named;
import javax.inject.Singleton;

import java.util.regex.Pattern;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;

public class NumberPoolModule extends AbstractModule
{
    public static final Pattern RANGE_PATTERN = Pattern.compile("(?<min>\\d+) ?- ?(?<max>\\d+)");

    public static final String MINOR_NUMBER_POOL = "MinorNumberPool";
    public static final String SPECIAL_SATELLTE_PORT_POOL = "SpecStltPortPool";
    public static final String LAYER_RSC_ID_POOL = "LayerRscIdPool";
    public static final String BACKUP_SHIPPING_PORT_POOL = "BackupShippingPortPool";

    private static final String MINOR_NR_ELEMENT_NAME = "Minor number";

    // we will load the ranges from the database, but if the database contains
    // invalid ranges (e.g. -1 for port), we will fall back to these defaults
    private static final int DEFAULT_MINOR_NR_MIN = 1000;
    private static final int DEFAULT_MINOR_NR_MAX = 49999;

    private static final int DEFAULT_SPEC_STLT_TCP_PORT_MIN = 10_000;
    private static final int DEFAULT_SPEC_STLT_TCP_PORT_MAX = 10_999;

    private static final String SPEC_STLT_TCP_ELEMENT_NAME = "Special Satellite TCP port";

    private static final int LAYER_RSC_ID_MIN = 0;
    private static final int LAYER_RSC_ID_MAX = BitmapPool.MAX_CAPACITY - 1;
    private static final String LAYER_RSC_ID_ELEMENT_NAME = "Layer Resource Id";

    private static final int DEFAULT_BACK_SHIP_PORT_MIN = 12_000;
    private static final int DEFAULT_BACK_SHIP_PORT_MAX = 12_999;
    private static final String BACK_SHIP_PORT_ELEMENT_NAME = "Backup Shipping Port";

    @Override
    protected void configure()
    {
    }

    @Provides
    @Singleton
    @Named(MINOR_NUMBER_POOL)
    public DynamicNumberPool minorNrPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps ctrlConfRef
    )
    {
        return new DynamicNumberPoolImpl(
            errorReporter,
            new PriorityProps(ctrlConfRef),
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
    @Named(SPECIAL_SATELLTE_PORT_POOL)
    public DynamicNumberPool specStltPortPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps ctrlConfRef
    )
    {
        return new DynamicNumberPoolImpl(
            errorReporter,
            new PriorityProps(ctrlConfRef),
            ApiConsts.KEY_SPEC_STLT_PORT_AUTO_RANGE,
            SPEC_STLT_TCP_ELEMENT_NAME,
            TcpPortNumber::tcpPortNrCheck,
            TcpPortNumber.PORT_NR_MAX,
            DEFAULT_SPEC_STLT_TCP_PORT_MIN,
            DEFAULT_SPEC_STLT_TCP_PORT_MAX
        );
    }

    @Provides
    @Singleton
    @Named(LAYER_RSC_ID_POOL)
    public DynamicNumberPool layerRscIdPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps ctrlConfRef
    )
    {
        return new DynamicNumberPoolImpl(
            errorReporter,
            new PriorityProps(ctrlConfRef),
            null,
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

    @Provides
    @Singleton
    @Named(BACKUP_SHIPPING_PORT_POOL)
    public DynamicNumberPool snapshotBackupPortPool(
        ErrorReporter errorReporter,
        @Named(LinStor.CONTROLLER_PROPS) ReadOnlyProps ctrlConfRef
    )
    {
        return new DynamicNumberPoolImpl(
            errorReporter,
            new PriorityProps(ctrlConfRef),
            ApiConsts.NAMESPC_SNAPSHOT_SHIPPING + "/" + ApiConsts.KEY_TCP_PORT_RANGE,
            BACK_SHIP_PORT_ELEMENT_NAME,
            TcpPortNumber::tcpPortNrCheck,
            TcpPortNumber.PORT_NR_MAX,
            DEFAULT_BACK_SHIP_PORT_MIN,
            DEFAULT_BACK_SHIP_PORT_MAX
        );
    }
}
