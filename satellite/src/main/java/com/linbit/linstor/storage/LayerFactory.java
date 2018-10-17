package com.linbit.linstor.storage;

import com.linbit.extproc.ExtCmdFactory;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.api.prop.WhitelistProps;
import com.linbit.linstor.core.ControllerPeerConnector;
import com.linbit.linstor.core.DeviceManager;
import com.linbit.linstor.core.StltConfigAccessor;
import com.linbit.linstor.drbdstate.DrbdStateStore;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.storage.layer.DeviceLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.DrbdLayer;
import com.linbit.linstor.storage.layer.adapter.drbd.utils.DrbdAdm;
import com.linbit.linstor.storage.layer.provider.StorageLayer;
import com.linbit.linstor.storage2.layer.kinds.DeviceLayerKind;
import com.linbit.linstor.storage2.layer.kinds.DrbdLayerKind;
import com.linbit.linstor.storage2.layer.kinds.StorageLayerKind;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;
import java.util.HashMap;
import java.util.Map;

public class LayerFactory
{
    private final Map<Class<? extends DeviceLayerKind>, DeviceLayer> devLayerLookupTable;

    public LayerFactory(
        AccessContext workerCtx,
        DeviceManager notificationListener,
        DrbdAdm drbdUtils,
        DrbdStateStore drbdState,
        ErrorReporter errorReporter,
        WhitelistProps whiltelistProps,
        ExtCmdFactory extCmdFactory,
        StltConfigAccessor stltConfigAccessor,
        Provider<TransactionMgr> transMgrProvider,
        CtrlStltSerializer interComSerializer,
        ControllerPeerConnector controllerPeerConnector
    )
    {
        devLayerLookupTable = new HashMap<>();
        devLayerLookupTable.put(
            DrbdLayerKind.class,
            new DrbdLayer(
                workerCtx,
                notificationListener,
                drbdUtils,
                drbdState,
                errorReporter,
                whiltelistProps,
                interComSerializer,
                controllerPeerConnector
            )
        );
        devLayerLookupTable.put(
            StorageLayerKind.class,
            new StorageLayer(
                extCmdFactory,
                workerCtx,
                stltConfigAccessor,
                errorReporter,
                transMgrProvider,
                notificationListener
            )
        );
    }

    public DeviceLayer getDeviceLayer(Class<? extends DeviceLayerKind> kindClass)
    {
        return devLayerLookupTable.get(kindClass);
    }
}
