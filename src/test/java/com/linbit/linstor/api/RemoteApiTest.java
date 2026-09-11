package com.linbit.linstor.api;

import com.linbit.linstor.api.pojo.LinstorRemotePojo;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlConfApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlRemoteApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.RemoteName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.core.objects.remotes.AbsRemote;
import com.linbit.linstor.core.objects.remotes.EbsRemote;
import com.linbit.linstor.core.objects.remotes.LinstorRemote;
import com.linbit.linstor.core.objects.remotes.S3Remote;
import com.linbit.linstor.netcom.Peer;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.List;

import com.amazonaws.SdkClientException;
import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteApiTest extends ApiTestBase
{
    private static final String TEST_REMOTE_NAME = "testRemote";
    private static final String MASTER_PASSPHRASE = "topSecret";

    // the remote API handler uses a modify operation context for all its operations
    private static final long RC_REMOTE_CREATED =
        ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.CREATED;
    private static final long RC_REMOTE_MODIFIED =
        ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.MODIFIED;
    private static final long RC_REMOTE_DELETED =
        ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.DELETED;

    @Inject
    private Provider<CtrlRemoteApiCallHandler> remoteApiCallHandlerProvider;

    @Inject
    private Provider<CtrlConfApiCallHandler> ctrlConfApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Bind
    @Mock
    protected BackupToS3 backupToS3;

    @Mock
    protected Peer mockSatellite;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        leaveScope();
    }

    /*
     * Linstor remotes
     */

    @Test
    public void createLinstorRemoteSuccess() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createLinstor(TEST_REMOTE_NAME, "10.0.0.1", null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_REMOTE_CREATED, rc.get(0));

        LinstorRemote remote = getRemote(LinstorRemote.class);
        // protocol and port are filled in with defaults
        assertThat(remote.getUrl().toString()).isEqualTo("http://10.0.0.1:3370");
        assertThat(remote.getEncryptedRemotePassphrase()).isNull();
        assertThat(remote.getClusterId()).isNull();

        List<LinstorRemotePojo> linstorRemotes = remoteApiCallHandlerProvider.get().listLinstor();
        assertThat(linstorRemotes).hasSize(1);
        assertThat(linstorRemotes.get(0).getRemoteName()).isEqualTo(TEST_REMOTE_NAME);
        assertThat(remoteApiCallHandlerProvider.get().listS3()).isEmpty();
        assertThat(remoteApiCallHandlerProvider.get().listEbs()).isEmpty();
    }

    @Test
    public void createLinstorRemoteWithClusterIdAndPassphrase() throws Exception
    {
        setMasterPassphrase();
        java.util.UUID clusterId = java.util.UUID.randomUUID();

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createLinstor(TEST_REMOTE_NAME, "https://otherctrl", "otherPassphrase", clusterId.toString())
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_REMOTE_CREATED, rc.get(0));

        LinstorRemote remote = getRemote(LinstorRemote.class);
        assertThat(remote.getUrl().toString()).isEqualTo("https://otherctrl:3371");
        assertThat(remote.getEncryptedRemotePassphrase()).isNotEmpty();
        assertThat(remote.getClusterId()).isEqualTo(clusterId);
    }

    @Test
    public void createLinstorRemoteWithPassphraseWithoutMasterKeyFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createLinstor(TEST_REMOTE_NAME, "10.0.0.1", "otherPassphrase", null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
            rc.get(0)
        );
    }

    @Test
    public void createLinstorRemoteMissingUrlFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createLinstor(TEST_REMOTE_NAME, "", null, null)
        );
        assertThat(rc).hasSize(1);
        // the missing parameter check reports with the backup object mask instead of the remote mask
        expectRc(
            0,
            ApiConsts.MASK_BACKUP | ApiConsts.MASK_MOD | ApiConsts.FAIL_INVLD_BACKUP_CONFIG,
            rc.get(0)
        );
        assertThat(rc.get(0).getMessage()).contains("url");
    }

    @Test
    public void createLinstorRemoteInvalidNameFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createLinstor("invalid name", "10.0.0.1", null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_INVLD_REMOTE_NAME,
            rc.get(0)
        );
    }

    @Test
    public void createLinstorRemoteInvalidClusterIdFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createLinstor(TEST_REMOTE_NAME, "10.0.0.1", null, "notAUuid")
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_INVLD_CONF,
            rc.get(0)
        );
        assertThat(remoteMap.get(new RemoteName(TEST_REMOTE_NAME))).isNull();
    }

    @Test
    public void createLinstorRemoteDuplicateNameFails() throws Exception
    {
        createDefaultLinstorRemote();

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createLinstor(TEST_REMOTE_NAME, "10.0.0.2", null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_EXISTS_REMOTE,
            rc.get(0)
        );
    }

    @Test
    public void modifyLinstorRemoteSuccess() throws Exception
    {
        createDefaultLinstorRemote();
        java.util.UUID clusterId = java.util.UUID.randomUUID();

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .changeLinstor(TEST_REMOTE_NAME, "https://192.168.0.1", null, clusterId.toString())
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_REMOTE_MODIFIED, rc.get(0));

        LinstorRemote remote = getRemote(LinstorRemote.class);
        assertThat(remote.getUrl().toString()).isEqualTo("https://192.168.0.1:3371");
        assertThat(remote.getClusterId()).isEqualTo(clusterId);
    }

    @Test
    public void modifyUnknownRemoteFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().changeLinstor("unknownRemote", "10.0.0.1", null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_NOT_FOUND_REMOTE,
            rc.get(0)
        );
    }

    @Test
    public void modifyLinstorRemoteInvalidClusterIdFails() throws Exception
    {
        createDefaultLinstorRemote();

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().changeLinstor(TEST_REMOTE_NAME, null, null, "notAUuid")
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_INVLD_CONF,
            rc.get(0)
        );
    }

    @Test
    public void modifyLinstorRemoteWrongTypeFails() throws Exception
    {
        createDefaultS3Remote();

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().changeLinstor(TEST_REMOTE_NAME, "10.0.0.1", null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_EXISTS_REMOTE,
            rc.get(0)
        );
    }

    @Test
    public void deleteLinstorRemoteSuccess() throws Exception
    {
        createDefaultLinstorRemote();

        ApiCallRc rc = collect(remoteApiCallHandlerProvider.get().delete(TEST_REMOTE_NAME));
        assertThat(rc).hasSize(2);
        // "marked for deletion", followed by the actual deletion in a second transaction
        expectRc(0, RC_REMOTE_DELETED, rc.get(0));
        expectRc(1, RC_REMOTE_DELETED, rc.get(1));

        assertThat(remoteMap.get(new RemoteName(TEST_REMOTE_NAME))).isNull();
        assertThat(remoteApiCallHandlerProvider.get().listLinstor()).isEmpty();
    }

    @Test
    public void deleteUnknownRemoteWarns() throws Exception
    {
        ApiCallRc rc = collect(remoteApiCallHandlerProvider.get().delete("unknownRemote"));
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.WARN_NOT_FOUND,
            rc.get(0)
        );
    }

    @Test
    public void createUrlWithDefaultsAppliesProtocolAndPort()
    {
        assertThat(CtrlRemoteApiCallHandler.createUrlWithDefaults("10.0.0.1").toString())
            .isEqualTo("http://10.0.0.1:3370");
        assertThat(CtrlRemoteApiCallHandler.createUrlWithDefaults("https://somehost").toString())
            .isEqualTo("https://somehost:3371");
        assertThat(CtrlRemoteApiCallHandler.createUrlWithDefaults("http://somehost:8080").toString())
            .isEqualTo("http://somehost:8080");
    }

    /*
     * S3 remotes
     */

    @Test
    public void createS3RemoteSuccess() throws Exception
    {
        setMasterPassphrase();

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createS3(
                TEST_REMOTE_NAME,
                "s3.example.com",
                "testBucket",
                "us-east-1",
                "accessKey",
                "secretKey",
                true
            )
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_REMOTE_CREATED, rc.get(0));

        S3Remote remote = getRemote(S3Remote.class);
        assertThat(remote.getUrl()).isEqualTo("s3.example.com");
        assertThat(remote.getBucket()).isEqualTo("testBucket");
        assertThat(remote.getRegion()).isEqualTo("us-east-1");
        assertThat(remote.getAccessKey()).isNotEmpty();
        assertThat(remote.getSecretKey()).isNotEmpty();
        assertThat(remote.getFlags().isSet(AbsRemote.Flags.S3_USE_PATH_STYLE)).isTrue();

        // the connection check must have been performed
        Mockito.verify(backupToS3).listObjects(Mockito.eq(""), Mockito.eq(remote), Mockito.any());

        assertThat(remoteApiCallHandlerProvider.get().listS3()).hasSize(1);
        assertThat(remoteApiCallHandlerProvider.get().listS3().get(0).getRemoteName())
            .isEqualTo(TEST_REMOTE_NAME);
    }

    @Test
    public void createS3RemoteMissingParametersFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createS3(TEST_REMOTE_NAME, "", "testBucket", "", "accessKey", "", false)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_BACKUP | ApiConsts.MASK_MOD | ApiConsts.FAIL_INVLD_BACKUP_CONFIG,
            rc.get(0)
        );
        assertThat(rc.get(0).getMessage())
            .contains("endpoint")
            .contains("region")
            .contains("secret_key");
    }

    @Test
    public void createS3RemoteWithoutMasterPassphraseFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createS3(
                TEST_REMOTE_NAME,
                "s3.example.com",
                "testBucket",
                "us-east-1",
                "accessKey",
                "secretKey",
                false
            )
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_NOT_FOUND_CRYPT_KEY,
            rc.get(0)
        );
    }

    @Test
    public void createS3RemoteUnreachableFails() throws Exception
    {
        setMasterPassphrase();
        Mockito.when(backupToS3.listObjects(Mockito.anyString(), Mockito.any(S3Remote.class), Mockito.any()))
            .thenThrow(new SdkClientException("test: endpoint unreachable"));

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createS3(
                TEST_REMOTE_NAME,
                "s3.example.com",
                "testBucket",
                "us-east-1",
                "accessKey",
                "secretKey",
                false
            )
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_BACKUP | ApiConsts.MASK_MOD | ApiConsts.FAIL_UNKNOWN_ERROR,
            rc.get(0)
        );
    }

    @Test
    public void modifyS3RemoteSuccess() throws Exception
    {
        createDefaultS3Remote();

        // a successful S3 remote modification currently only responds with satellite update answers
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .changeS3(TEST_REMOTE_NAME, "minio.example.com", "otherBucket", "eu-west-1", null, null)
        );
        assertThat(rc).isEmpty();

        S3Remote remote = getRemote(S3Remote.class);
        assertThat(remote.getUrl()).isEqualTo("minio.example.com");
        assertThat(remote.getBucket()).isEqualTo("otherBucket");
        assertThat(remote.getRegion()).isEqualTo("eu-west-1");
    }

    @Test
    public void modifyS3RemoteUnknownFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .changeS3("unknownRemote", null, "otherBucket", null, null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_NOT_FOUND_REMOTE,
            rc.get(0)
        );
    }

    @Test
    public void deleteS3RemoteSuccess() throws Exception
    {
        createDefaultS3Remote();
        S3Remote remote = getRemote(S3Remote.class);

        ApiCallRc rc = collect(remoteApiCallHandlerProvider.get().delete(TEST_REMOTE_NAME));
        assertThat(rc).hasSize(2);
        expectRc(0, RC_REMOTE_DELETED, rc.get(0));
        expectRc(1, RC_REMOTE_DELETED, rc.get(1));

        assertThat(remoteMap.get(new RemoteName(TEST_REMOTE_NAME))).isNull();
        Mockito.verify(backupToS3).deleteRemoteFromCache(remote);
    }

    /*
     * EBS remotes
     */

    @Test
    public void createEbsRemoteSuccess() throws Exception
    {
        setMasterPassphrase();

        // region and endpoint are derived from the availability zone
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createEbs(TEST_REMOTE_NAME, null, null, "eu-west-1a", "accessKey", "secretKey")
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_REMOTE_CREATED, rc.get(0));

        EbsRemote remote = getRemote(EbsRemote.class);
        assertThat(remote.getAvailabilityZone()).isEqualTo("eu-west-1a");
        assertThat(remote.getRegion()).isEqualTo("eu-west-1");
        assertThat(remote.getUrl().toString()).isEqualTo("https://ec2.eu-west-1.amazonaws.com");
        assertThat(remote.getDecryptedAccessKey()).isEqualTo("accessKey");
        assertThat(remote.getDecryptedSecretKey()).isEqualTo("secretKey");

        assertThat(remoteApiCallHandlerProvider.get().listEbs()).hasSize(1);
    }

    @Test
    public void createEbsRemoteMissingParametersFails() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createEbs(TEST_REMOTE_NAME, null, null, "", "accessKey", "")
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_BACKUP | ApiConsts.MASK_MOD | ApiConsts.FAIL_INVLD_BACKUP_CONFIG,
            rc.get(0)
        );
        assertThat(rc.get(0).getMessage())
            .contains("availability_zone")
            .contains("secret_key");
    }

    @Test
    public void createEbsRemoteUnparsableAvailabilityZoneFails() throws Exception
    {
        // without a region, the region has to be derivable from the availability zone
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createEbs(TEST_REMOTE_NAME, null, null, "badAz", "accessKey", "secretKey")
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_MISSING_EBS_TARGET,
            rc.get(0)
        );
    }

    @Test
    public void createEbsRemoteInvalidEndpointFails() throws Exception
    {
        setMasterPassphrase();

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .createEbs(TEST_REMOTE_NAME, "notAUrl", null, "eu-west-1a", "accessKey", "secretKey")
        );
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_INVLD_CONF,
            rc.get(0)
        );
    }

    @Test
    public void modifyEbsRemoteAvailabilityZoneUpdatesRegionAndEndpoint() throws Exception
    {
        setMasterPassphrase();
        collect(
            remoteApiCallHandlerProvider.get()
                .createEbs(TEST_REMOTE_NAME, null, null, "eu-west-1a", "accessKey", "secretKey")
        );

        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get()
                .changeEbs(TEST_REMOTE_NAME, null, null, "us-east-2b", null, null)
        );
        // main response plus one entry each for the automatically updated region and endpoint
        assertThat(rc).hasSize(3);
        expectRc(0, RC_REMOTE_MODIFIED, rc.get(0));
        expectRc(1, RC_REMOTE_MODIFIED, rc.get(1));
        expectRc(2, RC_REMOTE_MODIFIED, rc.get(2));

        EbsRemote remote = getRemote(EbsRemote.class);
        assertThat(remote.getAvailabilityZone()).isEqualTo("us-east-2b");
        assertThat(remote.getRegion()).isEqualTo("us-east-2");
        assertThat(remote.getUrl().toString()).isEqualTo("https://ec2.us-east-2.amazonaws.com");
    }

    @Test
    public void deleteEbsRemoteInUseFails() throws Exception
    {
        setMasterPassphrase();

        enterScope();
        Node node = nodeTestFactory.get("node1", true);
        node.setPeer(mockSatellite);
        nodesMap.put(new NodeName("node1"), node);
        StorPool storPool = storPoolTestFactory.get("node1", "ebsPool", true);
        storPool.getProps().setProp(
            ApiConsts.KEY_REMOTE,
            TEST_REMOTE_NAME,
            ApiConsts.NAMESPC_STORAGE_DRIVER + "/" + ApiConsts.NAMESPC_EBS
        );
        leaveScope();

        collect(
            remoteApiCallHandlerProvider.get()
                .createEbs(TEST_REMOTE_NAME, null, null, "eu-west-1a", "accessKey", "secretKey")
        );

        ApiCallRc rc = collect(remoteApiCallHandlerProvider.get().delete(TEST_REMOTE_NAME));
        assertThat(rc).hasSize(1);
        expectRc(
            0,
            ApiConsts.MASK_REMOTE | ApiConsts.MASK_MOD | ApiConsts.FAIL_IN_USE,
            rc.get(0)
        );
        assertThat(remoteMap.get(new RemoteName(TEST_REMOTE_NAME))).isNotNull();
    }

    private void createDefaultLinstorRemote() throws Exception
    {
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createLinstor(TEST_REMOTE_NAME, "10.0.0.1", null, null)
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_REMOTE_CREATED, rc.get(0));
    }

    private void createDefaultS3Remote() throws Exception
    {
        setMasterPassphrase();
        ApiCallRc rc = collect(
            remoteApiCallHandlerProvider.get().createS3(
                TEST_REMOTE_NAME,
                "s3.example.com",
                "testBucket",
                "us-east-1",
                "accessKey",
                "secretKey",
                false
            )
        );
        assertThat(rc).hasSize(1);
        expectRc(0, RC_REMOTE_CREATED, rc.get(0));
    }

    private void setMasterPassphrase() throws Exception
    {
        ApiCallRc rc = collect(ctrlConfApiCallHandlerProvider.get().setPassphrase(MASTER_PASSPHRASE, null));
        assertThat(rc).hasSize(1);
        expectRc(0, ApiConsts.MASK_CTRL_CONF | ApiConsts.MASK_CRT | ApiConsts.CREATED, rc.get(0));
    }

    private <REMOTE_TYPE extends AbsRemote> REMOTE_TYPE getRemote(Class<REMOTE_TYPE> clazz) throws Exception
    {
        AbsRemote remote = remoteMap.get(new RemoteName(TEST_REMOTE_NAME));
        assertThat(remote).isInstanceOf(clazz);
        return clazz.cast(remote);
    }
}
