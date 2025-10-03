package com.linbit.linstor;

import com.linbit.linstor.api.ApiConsts;

public class InternalApiConsts
{
    public static final String API_AUTH        = "Auth";
    public static final String API_AUTH_RESPONSE = "AuthResponse";

    public static final String API_FULL_SYNC_DATA     = "FullSyncData";
    public static final String API_FULL_SYNC_RESPONSE = "FullSyncResponse";

    public static final String API_CHANGED_DATA = "ChangedData"; // this constant should replace in the long run all
    // other API_CHANGED_* usages
    public static final String API_REQUEST_DATA = "RequestData"; // this constant should replace in the long run all
    // other API_REQUEST_* usages
    public static final String API_APPLY_DATA = "ApplyData"; // this constant should replace in the long run all
    // other API_APPLY_* (including API_APPLY_*_DELETED) usages

    public static final String API_CHANGED_CONTROLLER   = "ChangedController";
    public static final String API_REQUEST_CONTROLLER   = "RequestController";
    public static final String API_APPLY_CONTROLLER     = "ApplyController";
    public static final String API_OTHER_CONTROLLER     = "OtherController"; // old controller was replaced
    // with a new controller. Indicates that the old controller should not try to reconnect to satellite

    public static final String API_NOTIFY_NODE_APPLIED    = "NotifyNodeApplied";
    public static final String API_NOTIFY_NODE_FAILED    = "NotifyNodeFailed";
    public static final String API_CHANGED_NODE       = "ChangedNode";
    public static final String API_REQUEST_NODE       = "RequestNode";
    public static final String API_APPLY_NODE         = "ApplyNode";
    public static final String API_APPLY_NODE_DELETED = "ApplyDeletedNode";
    public static final String API_UPDATE_LOCAL_PROPS_FROM_STLT = "ApplyPropsFromStlt";

    public static final String API_NOTIFY_VLM_DRBD_RESIZED = "NotifyDrbdVlmResized";
    public static final String API_NOTIFY_RSC_APPLIED    = "NotifyRscApplied";
    public static final String API_NOTIFY_RSC_FAILED     = "NotifyRscFailed";
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
    public static final String API_NOTIFY_SNAPSHOT_ROLLBACK_RESULT  = "SnapshotRollbackResult";

    public static final String API_CHANGED_STOR_POOL       = "ChangedStorPool";
    public static final String API_REQUEST_STOR_POOL       = "RequestStorPool";
    public static final String API_APPLY_STOR_POOL         = "ApplyStorPool";
    public static final String API_APPLY_STOR_POOL_DELETED = "ApplyDeletedStorPool";
    public static final String API_UPDATE_FREE_CAPACITY    = "UpdateFreeCapacity";
    public static final String API_NOTIFY_STOR_POOL_APPLIED  = "NotifyStorPoolApplied";

    public static final String API_REQUEST_SHARED_SP_LOCKS = "RequestSharedStorPoolLocks";
    public static final String API_APPLY_SHARED_STOR_POOL_LOCKS = "ApplySharedStorPoolLocks";
    public static final String API_NOTIFY_DEV_MGR_RUN_COMPLETED = "NotifyDevMgrRunCompleted";

    public static final String API_CRYPT_KEY = "cryptKey";

    public static final String API_REQUEST_THIN_FREE_SPACE = "RequestThinFreeSpace";
    public static final String API_REQUEST_VLM_ALLOCATED = "RequestVlmAllocated";

    public static final String API_ARCHIVE_LOGS = "ArchiveLogs";

    public static final String API_LIST_PHYSICAL_DEVICES = "ListPhysicalDevices";
    public static final String API_ANSWER_PHYSICAL_DEVICES = "AnswerPhysicalDevices";
    public static final String API_CREATE_DEVICE_POOL = "CreateDevicePool";
    public static final String API_DELETE_DEVICE_POOL = "DeleteDevicePool";

    public static final String API_LST_STLT_CONFIG = "LstStltConfig";
    public static final String API_MOD_CONFIG = "ModifyConfig";
    public static final String API_MOD_STLT_CONFIG = "ModifyStltConfig";
    public static final String API_MOD_STLT_CONFIG_RESP = "ModifyStltConfigResp";

    public static final String API_REQ_SOS_REPORT_FILE_LIST = "RequestSosReportFileList";
    public static final String API_REQ_SOS_REPORT_FILES = "RequestSosReportFiles";
    public static final String API_RSP_SOS_REPORT_FILE_LIST = "ResponseSosReportFileList";
    public static final String API_RSP_SOS_REPORT_FILES = "ResponseSosReportFiles";
    public static final String API_REQ_SOS_REPORT_CLEANUP = "CleanupSosReport";
    public static final String API_RSP_SOS_REPORT_CLEANUP_FINISHED = "ResponseSosReportFinished";

    public static final String API_CHANGED_EXTERNAL_FILE = "ChangedExternalFile";
    public static final String API_REQUEST_EXTERNAL_FILE = "RequestExternalFile";
    public static final String API_APPLY_EXTERNAL_FILE = "ApplyExternalFile";
    public static final String API_APPLY_DELETED_EXTERNAL_FILE = "ApplyDeletedExternalFile";
    public static final String API_NOTIFY_EXTERNAL_FILE_APPLIED = "NotifyExternalFileApplied";
    /*
     * Event stream actions
     */
    public static final String EVENT_STREAM_VALUE = "Value";
    public static final String EVENT_STREAM_CLOSE_NO_CONNECTION = "CloseNoConnection";
    public static final String EVENT_STREAM_CLOSE_REMOVED = "CloseRemoved";

    /*
     * Events
     */
    public static final String EVENT_VOLUME_DISK_STATE = "VlmDiskState";
    public static final String EVENT_REPLICATION_STATE = "ReplicationState";
    public static final String EVENT_DONE_PERCENTAGE = "DonePercentageEvent";
    // State of resource based on DRBD status (or other underlying system)
    public static final String EVENT_RESOURCE_STATE = "ResourceState";
    public static final String EVENT_CONNECTION_STATE = "ConnectionState";

    public static final long API_AUTH_ERROR_HOST_MISMATCH = 1;

    public static final String PROP_PRIMARY_SET     = "DrbdPrimarySetOn";
    public static final String PROP_NVME_TARGET_NODE_NAME = "NvmeTargetNodeName";

    /*
     * Snapshot shipping
     */
    @Deprecated(forRemoval = true)
    public static final String API_NOTIFY_SNAPSHOT_SHIPPING_RECEIVED = "SnapshotShippingReceived";
    @Deprecated(forRemoval = true)
    public static final String KEY_SNAPSHOT_SHIPPING_NAME_IN_PROGRESS = "SnapshotShippingNameInProgress";
    @Deprecated(forRemoval = true)
    public static final String KEY_SNAPSHOT_SHIPPING_NAME_PREV = "SnapshotShippingNamePrev";
    @Deprecated(forRemoval = true)
    public static final String KEY_SNAPSHOT_SHIPPING_TARGET_NODE = "Shipping/Target";
    @Deprecated(forRemoval = true)
    public static final String KEY_SNAPSHOT_SHIPPING_SOURCE_NODE = "Shipping/Source";
    @Deprecated(forRemoval = true)
    public static final String KEY_SNAPSHOT_SHIPPING_PORT = "Shipping/Port";
    @Deprecated(forRemoval = true)
    public static final String KEY_SNAPSHOT_SHIPPING_PREF_TARGET_NIC = "Shipping/PrefTargetNic";
    @Deprecated(forRemoval = true)
    public static final String KEY_SNAPSHOT_SHIPPING_NEXT_ID = "SnapshotShippingNextId";

    /*
     * Backup shipping
     */
    public static final String API_NOTIFY_BACKUP_SHIPPING_RECEIVED = "BackupShippingReceived";
    public static final String API_NOTIFY_BACKUP_SHIPPING_SENT = "BackupShippingSent";
    public static final String API_NOTIFY_BACKUP_SHIPPING_ID = "BackupShippingId";
    public static final String API_NOTIFY_BACKUP_SHIPPING_WRONG_PORTS = "BackupShippingWrongPorts";
    public static final String API_NOTIFY_BACKUP_RCV_READY = "BackupRcvReady";
    public static final String API_NOTIFY_BACKUP_SHIPPING_FINISHED = "BackupShippingFinished";
    public static final String KEY_LAST_FULL_BACKUP_TIMESTAMP = "LastFullBackupTimestamp";
    public static final String KEY_BACKUP_LAST_SNAPSHOT = "BackupLastSnapshot";
    public static final String KEY_BACKUP_LAST_STARTED_OR_QUEUED = "BackupLastStartedOrQueued";
    public static final String KEY_BACKUP_TO_RESTORE = "BackupToRestore";
    public static final String KEY_BACKUP_NODE_IDS_TO_RESET = "BackupNodeIdsToReset";
    public static final String KEY_BACKUP_NODE_ID_SEPERATOR = ",";
    public static final String API_BACKUP_REST_START_RECEIVING = "BackupRestStartReceiving";
    public static final String API_BACKUP_REST_RECEIVING_DONE = "BackupRestReceivingDone";
    public static final String API_BACKUP_REST_PREPARE_ABORT = "BackupRestPrepareAbort";
    public static final String KEY_BACKUP_SHIP_PORT = "BackupShipPort";
    public static final String KEY_BACKUP_START_TIMESTAMP = "BackupStartTimestamp";

    public static final String KEY_BACKUP_SRC_REMOTE = "BackupSrcRemote";
    public static final String KEY_BACKUP_TARGET_REMOTE = "BackupTargetRemote";

    public static final String API_REQUEST_REMOTE = "RequestRemote";
    public static final String API_APPLY_DELETED_REMOTE = "ApplyDeletedRemote";
    public static final String API_APPLY_REMOTE = "ApplyRemote";
    public static final String API_CHANGED_REMOTE = "ChangedRemote";
    public static final String NAMESPC_REMOTE = "Remote";

    public static final String KEY_BACKUP_L2L_SRC_CLUSTER_UUID = "Backup/FromClusterUUID";
    public static final String KEY_BACKUP_L2L_SRC_CLUSTER_SHORT_HASH = "Backup/FromClusterName";
    public static final String KEY_BACKUP_L2L_SRC_SNAP_DFN_UUID = "Backup/Target/SourceSnapDfnUUID";

    public static final String NAMSPC_BACKUP_SRC_STOR_POOL_KINDS = "Backup/SourceStorPoolKinds";
    public static final String NAMESPC_SCHEDULE = "Schedule";
    public static final String KEY_TRIPLE_ENABLED = "Enabled";
    public static final String KEY_SCHEDULE_PREF_NODE = "PrefNode";
    public static final String KEY_FORCE_RESTORE = "ForceRestore";
    public static final String KEY_LAST_BACKUP_TIME = "LastBackupTime";
    public static final String KEY_LAST_BACKUP_INC = "LastBackupInc";
    public static final String KEY_BACKUP_SHIPPED_BY_SCHEDULE = "BackupShippedBySchedule";
    public static final String KEY_FORCE_INITIAL_SYNC_PERMA = "ForceInitialSyncPermanent";
    public static final String KEY_BACKUP_SRC_NODE = "SrcNode";
    public static final String KEY_BACKUP_DST_NODE = "DstNode";
    public static final String KEY_RENAME_STORPOOL_MAP = "RenameStorpoolMap";
    public static final String KEY_SCHEDULE_DST_RSC_GRP = "TargetResourceGroup";
    public static final String KEY_SCHEDULE_DST_RSC_NAME = "TargetResourceName";
    public static final String KEY_SCHEDULE_DST_RSC_GRP_FORCE = "ForceResourceGroup";

    public static final String KEY_BACKUP_TARGET = "Target";
    public static final String KEY_BACKUP_SOURCE = "Source";
    public static final String KEY_SHIPPING_STATUS = "ShippingStatus";
    public static final String VALUE_SHIPPING = "Shipping";
    public static final String VALUE_PREPARE_SHIPPING = "Prepare Shipping";
    public static final String VALUE_PREPARE_ABORT = "Prepare Abort";
    public static final String VALUE_ABORTING = "Aborting";
    public static final String VALUE_ABORTED = "Aborted";
    public static final String VALUE_SUCCESS = "Success";
    public static final String VALUE_FAILED = "Failed";
    public static final String KEY_SHIPPING_ERROR_REPORT = "ShippingErrorReport";
    public static final String KEY_ON_SUCCESS = "OnSuccess";
    public static final String VALUE_RESTORE = "Restore";
    public static final String VALUE_FORCE_RESTORE = "Force Restore";
    public static final String KEY_SHIPPING_NODE = "ShippingNode";

    /*
     * Cluster
     */
    public static final String KEY_CLUSTER_LOCAL_ID = "LocalID";

    // Normal module shutdown, no error
    public static final int EXIT_CODE_SHUTDOWN = 0;

    // Incorrect parameters on the command line
    public static final int EXIT_CODE_CMDLINE_ERROR = 1;

    // TOML parse error on startup
    public static final int EXIT_CODE_CONFIG_PARSE_ERROR = 2;

    // Startup failed because the default NetCom service could not be initialized
    public static final int EXIT_CODE_NETCOM_ERROR = 20;

    // Critical error triggered that would require restart of the satellite (uuid mismatch)
    public static final int EXIT_CODE_INTERNAL_CRITICAL_ERROR = 70;
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
    public static final String DEFAULT_RSC_GRP_NAME = "DfltRscGrp";
    public static final String NODE_UNAME = "NodeUname";
    public static final String DEFAULT_AUTO_SNAPSHOT_PREFIX = "autoSnap";
    public static final String SET_BY_VALUE_LINSTOR = "linstor";

    // drbd option consts
    public static final String NAMESPC_STLT_INTERNAL_DRBD = ApiConsts.NAMESPC_STLT + "/Drbd";
    public static final String NAMESPC_DRBD = "Drbd";
    public static final String DRBD_VERIFY_ALGO = "verify-alg";
    public static final String DRBD_AUTO_VERIFY_ALGO = "auto-verify-alg";
    public static final String KEY_DRBD_AUTO_VERIFY_ALGO_ALLOWED_LIST = "auto-verify-algo-allowed-list";
    public static final String KEY_DRBD_NODE_IDS_TO_RESET = "NodeIdsToReset";
    public static final String KEY_DRBD_QUORUM = "quorum";
    public static final String KEY_DRBD_NEEDS_INVALIDATE = "NeedsInvalidate";
    public static final String KEY_DRBD_BLOCK_SIZE = "block-size";

    // drbd actions
    public static final String MIN_IO_SIZE_RESTART_DRBD = ApiConsts.NAMESPC_STLT + "/minIoSizeRestartDrbd";

    // external files consts
    public static final String NAMESPC_FILES = "files";

    // cloning
    public static final String KEY_CLONED_FROM = "cloned-from";
    public static final String KEY_USE_ZFS_CLONE = "use-zfs-clone";
    public static final String API_NOTIFY_CLONE_UPDATE  = "NotifyCloneUpdate";
    public static final String CLONE_NS = "Clone";
    public static final String CLONE_FOR_PREFIX = "CF_";
    public static final String CLONE_PROP_PREFIX = CLONE_NS + "/" + CLONE_FOR_PREFIX;

    // ebs
    public static final String EBS_REMOTE_NAME = "EbsRemoteName";
    public static final String KEY_EBS_VLM_ID = "EbsVlmId";
    public static final String KEY_EBS_SNAP_ID = "EbsSnapId";
    public static final String EBS_DFTL_STOR_POOL_NAME = "EbsPool";
    public static final String KEY_EBS_CONNECTED_INIT_NODE_NAME = "ConnectedInitiator";
    public static final String KEY_EBS_COOLDOWN_UNTIL = "CooldownUntil";
    public static final String KEY_EBS_COOLDOWN_UNTIL_TIMESTAMP = "CooldownUntilTimestamp";

    // storage
    public static final String NAMESPC_STORAGE = "Storage";
    public static final String ALLOCATION_GRANULARITY = "AllocationGranularity";
    public static final String KEY_ZFS_RENAME_SUFFIX = "ZfsRenameSuffix";

    // lvm
    public static final String NAMESPC_LVM = "Lvm";
    public static final String KEY_LVM_STRIPES = "Stripes";
    public static final String VDO_POOL_SUFFIX = "-vdobase";

    // temporary context key
    public static final String ERR_IF_OFFLINE = "errIfOffline";

    private InternalApiConsts()
    {
    }
}
