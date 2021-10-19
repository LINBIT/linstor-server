package com.linbit.linstor.layer.storage.openflex.rest.responses;

import java.util.List;

public class OpenflexStatus
{
    public static final int CRITICAL_FAILURE = 25;
    public static final int COMPLETED = 8;

    public OpenflexStatusState State;
    public List<OpenflexStatusHealth> Health;
    public List<String> Details;

    public class OpenflexStatusState
    {
        public long ID;
        public String Name;
    }

    public static class OpenflexStatusHealth
    {
        public long ID;
        public String Name;
    }
}
