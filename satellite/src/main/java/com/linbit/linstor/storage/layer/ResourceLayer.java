package com.linbit.linstor.storage.layer;

import com.linbit.linstor.Resource;
import com.linbit.linstor.Snapshot;
import com.linbit.linstor.Volume;
import com.linbit.linstor.api.ApiCallRcImpl;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.layer.exceptions.ResourceException;
import com.linbit.linstor.storage.layer.exceptions.VolumeException;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public interface ResourceLayer
{
    String getName();

    void prepare(List<Resource> value, List<Snapshot> snapshots)
        throws StorageException, AccessDeniedException, SQLException;

    void updateGrossSize(Volume childVlm, Volume parentVolume)
        throws AccessDeniedException, SQLException;

    void process(Resource rsc, Collection<Snapshot> snapshots, ApiCallRcImpl apiCallRc)
        throws StorageException, ResourceException, VolumeException, AccessDeniedException,
            SQLException;

    void clearCache() throws StorageException;

    void setLocalNodeProps(Props localNodeProps);

}
