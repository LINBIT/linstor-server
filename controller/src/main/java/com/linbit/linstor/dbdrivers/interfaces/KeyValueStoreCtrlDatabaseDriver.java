package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.KeyValueStore;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;

public interface KeyValueStoreCtrlDatabaseDriver extends KeyValueStoreDatabaseDriver,
    ControllerDatabaseDriver<KeyValueStore, KeyValueStore.InitMaps, Void>
{

}
