package com.linbit.drbdmanage;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.UUID;

/**
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Resource
{
    public Resource create(
        AccessContext accCtx,
        ResourceDefinition resDfnRef,
        Node nodeRef,
        NodeId nodeId,
        SerialGenerator srlGen
    )
        throws AccessDeniedException;

    public UUID getUuid();

    public ObjectProtection getObjProt();

    public ResourceDefinition getDefinition();

    public Volume getVolume(VolumeNumber volNr);

    public Node getAssignedNode();

    public NodeId getNodeId();

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;
}
