package com.linbit.linstor;

public class InternalApiConsts
{
    public static final String API_AUTH        = "Auth";
    public static final String API_AUTH_RESPONSE = "AuthResponse";

    public static final String API_FULL_SYNC_DATA     = "FullSyncData";
    public static final String API_FULL_SYNC_RESPONSE = "FullSyncResponse";

    public static final String API_CHANGED_CONTROLLER   = "ChangedController";
    public static final String API_REQUEST_CONTROLLER   = "RequestController";
    public static final String API_APPLY_CONTROLLER     = "ApplyController";
    public static final String API_OTHER_CONTROLLER     = "OtherController"; // old controller was replaced
    // with a new controller. Indicates that the old controller should not try to reconnect to satellite

    public static final String API_CHANGED_NODE       = "ChangedNode";
    public static final String API_REQUEST_NODE       = "RequestNode";
    public static final String API_APPLY_NODE         = "ApplyNode";
    public static final String API_APPLY_NODE_DELETED = "ApplyDeletedNode";

    public static final String API_NOTIFY_VLM_DRBD_RESIZED = "NotifyDrbdVlmResized";
    public static final String API_NOTIFY_RSC_APPLIED    = "NotifyRscApplied";
    public static final String API_REQUEST_PRIMARY_RSC   = "RequestPrimaryRsc";
    public static final String API_PRIMARY_RSC           = "PrimaryRsc";

    public static final String API_CHANGED_RSC       = "ChangedRsc";
    public static final String API_REQUEST_RSC       = "RequestRsc";
    public static final String API_APPLY_RSC         = "ApplyRsc";
    public static final String API_APPLY_RSC_DELETED = "ApplyDeletedRsc";

    public static final String API_CHANGED_IN_PROGRESS_SNAPSHOT     = "ChangedInProgressSnapshot";
    public static final String API_REQUEST_IN_PROGRESS_SNAPSHOT     = "RequestInProgressSnapshot";
    public static final String API_APPLY_IN_PROGRESS_SNAPSHOT       = "ApplyInProgressSnapshot";
    // Satellites only track snapshots where something is happening - for instance, when they are being created or
    // deleted. This API call indicates that the action is completed; the underlying snapshot volume may still be
    // present.
    public static final String API_APPLY_IN_PROGRESS_SNAPSHOT_ENDED = "ApplyEndedInProgressSnapshot";

    public static final String API_CHANGED_STOR_POOL       = "ChangedStorPool";
    public static final String API_REQUEST_STOR_POOL       = "RequestStorPool";
    public static final String API_APPLY_STOR_POOL         = "ApplyStorPool";
    public static final String API_APPLY_STOR_POOL_DELETED = "ApplyDeletedStorPool";
    public static final String API_UPDATE_FREE_CAPACITY    = "UpdateFreeCapacity";

    public static final String API_CRYPT_KEY = "cryptKey";

    public static final String API_REQUEST_THIN_FREE_SPACE = "RequestThinFreeSpace";
    public static final String API_REQUEST_VLM_ALLOCATED = "RequestVlmAllocated";

    public static final long API_AUTH_ERROR_HOST_MISMATCH = 1;

    public static final String PROP_PRIMARY_SET     = "DrbdPrimarySetOn";

    // Normal module shutdown, no error
    public static final int EXIT_CODE_SHUTDOWN = 0;

    // Incorrect parameters on the command line
    public static final int EXIT_CODE_CMDLINE_ERROR = 1;

    // Startup failed because the default NetCom service could not be initialized
    public static final int EXIT_CODE_NETCOM_ERROR = 20;

    // Startup failed because DRBD is not usable / not installed
    // FIXME: This will probably not be a shutdown reason in future releases.
    //        In this case, the constant can be removed.
    public static final int EXIT_CODE_DRBD_ERROR = 21;

    // Shutdown caused by an unexpected exception, some unrecoverable error, or an implementation error
    public static final int EXIT_CODE_IMPL_ERROR = 199;

    // Number of peer slots for DRBD meta data if not specified in the corresponding property for the resource
    // definition or system-wide
    public static final short DEFAULT_PEER_SLOTS = 7;

    public static final String RSC_PROP_KEY_AUTO_SELECTED_STOR_POOL_NAME = "AutoSelectedStorPoolName";

    public static final String NAMESPC_INTERNAL = "Internal";
    public static final short DEFAULT_PEER_COUNT = 31;
    public static final long DEFAULT_AL_SIZE = 32;
    public static final int DEFAULT_AL_STRIPES = 1;
    public static final String DEFAULT_STOR_POOL_NAME = "DfltStorPool";

    private InternalApiConsts()
    {
    }
}
