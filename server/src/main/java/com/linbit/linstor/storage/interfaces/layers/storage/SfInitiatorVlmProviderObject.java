package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.storage.interfaces.layers.State;

public interface SfInitiatorVlmProviderObject extends SfVlmProviderObject
{
    State ATTACHABLE = new State(true, false, "Attachable");
    State WAITING_ATTACHABLE = new State(true, false, "Wf: Attachable");
    State WAITING_ATTACHABLE_TIMEOUT = new State(false, true, "To: Attachable");

    State ATTACHING = new State(true, false, "Attaching");
    State WAITING_ATTACHING_TIMEOUT = new State(false, true, "To: Attaching");

    State ATTACHED = new State(true, true, "Attached");

    State DETACHING = new State(true, false, "Detaching");
}
