package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.VolumeGroupData;
import com.linbit.linstor.dbdrivers.DatabaseException;

public interface VolumeGroupDataDatabaseDriver
{
    void persist(VolumeGroupData vlmGrpData) throws DatabaseException;

    void delete(VolumeGroupData vlmGrpData) throws DatabaseException;
}
