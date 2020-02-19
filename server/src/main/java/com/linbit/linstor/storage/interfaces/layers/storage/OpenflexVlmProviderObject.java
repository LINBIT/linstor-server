package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;

/**
 * Marker interface for holding common states between a potential OpenflexInitiatorVlmProviderObject
 * and {@link OpenflexTargetVlmProviderObject}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface OpenflexVlmProviderObject<RSC extends AbsResource<RSC>> extends VlmProviderObject<RSC>
{
    /** This state should never be seen by user, as it should only be used to close the vlmDiskStateEvent-stream */
    State INTERNAL_REMOVE = new State(true, true, "Removing");

    State FAILED = new State(false, true, "Failed");
    State IO_EXC = new State(false, true, "IO Exc");
}
