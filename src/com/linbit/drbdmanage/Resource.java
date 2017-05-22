package com.linbit.drbdmanage;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.propscon.SerialGenerator;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;
import java.util.Iterator;
import com.linbit.drbdmanage.stateflags.Flags;
import com.linbit.drbdmanage.stateflags.StateFlags;
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

    public Iterator<Volume> iterateVolumes();

    public Node getAssignedNode();

    public NodeId getNodeId();

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public StateFlags<RscFlags> getStateFlags();

    public enum RscFlags implements Flags
    {
        CLEAN(1L),
        REMOVE(2L);

        public static final RscFlags[] ALL_FLAGS =
        {
            CLEAN,
            REMOVE
        };

        public final long flagValue;

        private RscFlags(long value)
        {
            flagValue = value;
        }

        @Override
        public long getFlagValue()
        {
            return flagValue;
        }
    }
}
