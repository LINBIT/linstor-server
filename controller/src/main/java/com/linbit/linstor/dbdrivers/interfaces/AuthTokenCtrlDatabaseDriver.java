package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.AuthToken;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import java.time.Instant;

public interface AuthTokenCtrlDatabaseDriver extends ControllerDatabaseDriver<AuthToken, AuthToken.InitMaps, Void>
{
    SingleColumnDatabaseDriver<AuthToken, String> getDescriptionDriver();
    SingleColumnDatabaseDriver<AuthToken, Instant> getDeletedAtDriver();
    SingleColumnDatabaseDriver<AuthToken, String> getIpFilterDriver();
    SingleColumnDatabaseDriver<AuthToken, Boolean> getIsActiveDriver();
}
