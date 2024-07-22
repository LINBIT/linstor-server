package com.linbit.linstor.interfaces;

/*
 * TODO this interface should be in the satellite project, not server.
 *
 * The only reason it is still in the server-project are the serializer-classes. For some reason satellite specific
 * methods (i.e. methods that only the satellite can send to the controller, not vice versa) are still in the server-
 * project instead of the satellite project.
 */

import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.UUID;

public interface StorPoolInfo
{
    UUID getUuid();

    StorPoolName getName();

    NodeInfo getReadOnlyNode();

    DeviceProviderKind getDeviceProviderKind();

    ReadOnlyProps getReadOnlyProps(AccessContext accCtxRef) throws AccessDeniedException;
}
