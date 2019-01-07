package com.linbit.linstor.storage.layer.provider.swordfish;

import com.linbit.linstor.storage.layer.data.SfVlmLayerData;
import com.linbit.linstor.storage.layer.data.State;

import java.util.ArrayList;
import java.util.List;

public class SfVlmDataStlt implements SfVlmLayerData
{
    // target states
    public static final State CREATING = new State(true, false, "Creating");
    public static final State CREATING_TIMEOUT = new State(false, true, "To: Creating");

    public static final State CREATED = new State(true, true, "Created");

    // initiator states
    public static final State ATTACHABLE = new State(true, false, "Attachable");
    public static final State WAITING_ATTACHABLE = new State(true, false, "Wf: Attachable");
    public static final State WAITING_ATTACHABLE_TIMEOUT = new State(false, true, "To: Attachable");

    public static final State ATTACHING = new State(true, false, "Attaching");
    public static final State WAITING_ATTACHING_TIMEOUT = new State(false, true, "To: Attaching");

    public static final State ATTACHED = new State(true, true, "Attached");

    public static final State DETACHING = new State(true, false, "Detaching");

    // common states
    public static final State INTERNAL_REMOVE = new State(true, true, "Removing"); // should never be seen by user, as
    // this should only be used to close the vlmDiskStateEvent-stream

    public static final State FAILED = new State(false, true, "Failed");
    public static final State IO_EXC = new State(false, true, "IO Exc");

    SfVlmDfnDataStlt vlmDfnData;

    boolean isFailed = false;
    List<State> states = new ArrayList<>();

    SfVlmDataStlt(SfVlmDfnDataStlt vlmDfnDataRef)
    {
        vlmDfnData = vlmDfnDataRef;
    }

    @Override
    public boolean exists()
    {
        return vlmDfnData.exists;
    }

    @Override
    public boolean isFailed()
    {
        return isFailed;
    }

    @Override
    public List<State> getStates()
    {
        return states;
    }

}
