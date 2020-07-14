package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.dbdrivers.interfaces.updater.SingleColumnDatabaseDriver;

import java.util.Date;

public interface AbsResourceDatabaseDriver<RSC extends AbsResource<RSC>>
{
    SingleColumnDatabaseDriver<AbsResource<RSC>, Date> getCreateTimeDriver();
}
