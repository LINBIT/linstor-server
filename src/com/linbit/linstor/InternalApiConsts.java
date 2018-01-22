package com.linbit.linstor;

public class InternalApiConsts
{

    public static final String API_AUTH              = "Auth";
    public static final String API_AUTH_ACCEPT       = "AuthAccept";
    public static final String API_AUTH_ERROR        = "AuthError";

    public static final String API_FULL_SYNC_DATA    = "FullSyncData";
    public static final String API_UPDATE_STATES     = "UpdateStates";

    public static final String API_CHANGED_NODE      = "ChangedNode";
    public static final String API_REQUEST_NODE      = "RequestNode";
    public static final String API_APPLY_NODE        = "ApplyNode";

    public static final String API_CHANGED_RSC_DFN   = "ChangedRscDfn";
    public static final String API_REQUEST_RSC_DFN   = "RequestRscDfn";
    public static final String API_APPLY_RSC_DFN     = "ApplyRscDfn";
    public static final String API_NOTIFY_RSC_DEL    = "NotifyRscDel";
    public static final String API_NOTIFY_VLM_DEL    = "NotifyVlmDel";
    public static final String API_REQUEST_PRIMARY_RSC = "RequestPrimaryRsc";
    public static final String API_PRIMARY_RSC       = "PrimaryRsc";

    public static final String API_CHANGED_RSC       = "ChangedRsc";
    public static final String API_REQUEST_RSC       = "RequestRsc";
    public static final String API_APPLY_RSC         = "ApplyRsc";

    public static final String API_CHANGED_STOR_POOL = "ChangedStorPool";
    public static final String API_REQUEST_STOR_POOL = "RequestStorPool";
    public static final String API_APPLY_STOR_POOL   = "ApplyStorPool";

    public static final long API_AUTH_ERROR_HOST_MISMATCH = 1;

    public static final String PROP_PRIMARY_SET     = "DrbdPrimarySetOn";

    private InternalApiConsts()
    {
    }
}
