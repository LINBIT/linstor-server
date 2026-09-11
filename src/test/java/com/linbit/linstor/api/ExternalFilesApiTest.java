package com.linbit.linstor.api;

import com.linbit.linstor.InternalApiConsts;
import com.linbit.linstor.api.pojo.ExtFileStatusPojo;
import com.linbit.linstor.api.pojo.ExternalFilePojo;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlExternalFilesApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.apicallhandler.response.ApiRcException;
import com.linbit.linstor.core.cfg.StltConfig;
import com.linbit.linstor.core.identifier.ExternalFileName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.ExternalFile;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.ResourceDefinition;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.javainternal.s2c.MsgIntExtFileStatusOuterClass.MsgIntExtFileStatus;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class ExternalFilesApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String TEST_FILE_NAME = "/etc/test.conf";
    private static final byte[] TEST_CONTENT = "test content".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] OTHER_CONTENT = "other content".getBytes(StandardCharsets.US_ASCII);

    @Inject
    private Provider<CtrlExternalFilesApiCallHandler> extFilesApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected StltConfig mockStltConfig;

    private final NodeName testNodeName;
    private Node testNode;

    public ExternalFilesApiTest() throws Exception
    {
        testNodeName = new NodeName(TEST_NODE_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(mockSatellite.isOnline()).thenReturn(false);
        Mockito.when(mockSatellite.getConnectionStatus()).thenReturn(ApiConsts.ConnectionStatus.OFFLINE);

        testNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testNode);

        leaveScope();
    }

    /*
     * set / modify (flux based, must not run within the testScope).
     * A successful set only returns the satellite update responses, which are empty here since
     * the only satellite is offline.
     */

    @Test
    public void setCreatesFile() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );

        ExternalFile extFile = extFileMap.get(new ExternalFileName(TEST_FILE_NAME));
        assertThat(extFile).isNotNull();
        assertThat(extFile.getContent()).isEqualTo(TEST_CONTENT);
        assertThat(extFile.getAltSuffixes()).isEmpty();
    }

    @Test
    public void setSameContentAgainIsNoOp() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );
        ExternalFile extFile = extFileMap.get(new ExternalFileName(TEST_FILE_NAME));
        String checkSum = extFile.getContentCheckSumHex();

        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );

        assertThat(extFileMap.get(new ExternalFileName(TEST_FILE_NAME))).isSameAs(extFile);
        assertThat(extFile.getContent()).isEqualTo(TEST_CONTENT);
        assertThat(extFile.getContentCheckSumHex()).isEqualTo(checkSum);
    }

    @Test
    public void setModifiesContent() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );
        ExternalFile extFile = extFileMap.get(new ExternalFileName(TEST_FILE_NAME));
        String checkSum = extFile.getContentCheckSumHex();

        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, OTHER_CONTENT)
        );

        assertThat(extFile.getContent()).isEqualTo(OTHER_CONTENT);
        assertThat(extFile.getContentCheckSumHex()).isNotEqualTo(checkSum);
    }

    @Test
    public void setWithAltSuffixes() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
                .setAltSuffixes("-alt1", "_alt2")
        );

        ExternalFile extFile = extFileMap.get(new ExternalFileName(TEST_FILE_NAME));
        assertThat(extFile.getAltSuffixes()).containsExactly("-alt1", "_alt2");

        // update only the alternative suffixes, content stays untouched
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, null)
                .setAltSuffixes("-alt3")
        );

        assertThat(extFile.getAltSuffixes()).containsExactly("-alt3");
        assertThat(extFile.getContent()).isEqualTo(TEST_CONTENT);
    }

    @Test
    public void setEmptyContentOnExistingFileIsIgnored() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );

        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, new byte[0])
        );

        ExternalFile extFile = extFileMap.get(new ExternalFileName(TEST_FILE_NAME));
        assertThat(extFile.getContent()).isEqualTo(TEST_CONTENT);
    }

    @Test
    public void setNullContentForNewFileFails() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, null, ApiConsts.FAIL_INVLD_EXT_FILE)
        );

        assertThat(extFileMap.get(new ExternalFileName(TEST_FILE_NAME))).isNull();
    }

    @Test
    public void setEmptyContentForNewFileFails() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, new byte[0], ApiConsts.FAIL_INVLD_EXT_FILE)
        );

        assertThat(extFileMap.get(new ExternalFileName(TEST_FILE_NAME))).isNull();
    }

    @Test
    public void setRelativePathFails() throws Exception
    {
        evaluateTest(
            new SetExtFileCall("etc/relative.conf", TEST_CONTENT, ApiConsts.FAIL_INVLD_EXT_FILE_NAME)
        );

        assertThat(extFileMap).isEmpty();
    }

    @Test
    public void setInvalidAltSuffixFails() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT, ApiConsts.FAIL_INVLD_EXT_FILE)
                .setAltSuffixes("sub/path")
        );
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT, ApiConsts.FAIL_INVLD_EXT_FILE)
                .setAltSuffixes("..")
        );

        assertThat(extFileMap.get(new ExternalFileName(TEST_FILE_NAME))).isNull();
    }

    /*
     * delete (flux based)
     */

    @Test
    public void deleteExistingFile() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );

        evaluateTest(
            new DeleteExtFileCall(
                TEST_FILE_NAME,
                ApiConsts.DELETED, // marked for deletion
                ApiConsts.DELETED  // actually deleted
            )
        );

        assertThat(extFileMap).isEmpty();
    }

    @Test
    public void deleteNonexistentFileWarns() throws Exception
    {
        evaluateTest(
            new DeleteExtFileCall(TEST_FILE_NAME, ApiConsts.WARN_NOT_FOUND)
        );
    }

    @Test
    public void deleteInvalidNameFails() throws Exception
    {
        evaluateTest(
            new DeleteExtFileCall("relative/path.conf", ApiConsts.FAIL_INVLD_EXT_FILE_NAME)
        );
    }

    @Test
    public void deleteRemovesResourceDefinitionProperty() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );

        enterScope();
        ResourceDefinition rscDfn = resourceDefinitionTestFactory.builder("TestRsc").build();
        rscDfnMap.put(rscDfn.getName(), rscDfn);
        String extFilePropKey = InternalApiConsts.NAMESPC_FILES + "/" + TEST_FILE_NAME;
        rscDfn.getProps().setProp(extFilePropKey, ApiConsts.VAL_TRUE);
        leaveScope();

        evaluateTest(
            new DeleteExtFileCall(
                TEST_FILE_NAME,
                ApiConsts.DELETED,
                ApiConsts.DELETED
            )
        );

        assertThat(rscDfn.getProps().getProp(extFilePropKey)).isNull();
        assertThat(extFileMap).isEmpty();
    }

    /*
     * listFiles
     */

    @Test
    public void listFiles() throws Exception
    {
        evaluateTest(
            new SetExtFileCall(TEST_FILE_NAME, TEST_CONTENT)
        );
        evaluateTest(
            new SetExtFileCall("/etc/other.conf", OTHER_CONTENT)
        );

        List<ExternalFilePojo> allFiles = extFilesApiCallHandlerProvider.get().listFiles(ignored -> true);
        assertThat(allFiles).extracting(ExternalFilePojo::getFileName)
            .containsExactlyInAnyOrder(TEST_FILE_NAME, "/etc/other.conf");

        List<ExternalFilePojo> filtered = extFilesApiCallHandlerProvider.get()
            .listFiles(fileName -> fileName.equals(TEST_FILE_NAME));
        assertThat(filtered).hasSize(1);
        assertThat(filtered.get(0).getFileName()).isEqualTo(TEST_FILE_NAME);
        assertThat(filtered.get(0).getContent()).isEqualTo(TEST_CONTENT);
    }

    /*
     * getStatus
     */

    @Test
    public void getStatusParsesSatelliteResponse() throws Exception
    {
        MsgIntExtFileStatus statusMsg = MsgIntExtFileStatus.newBuilder()
            .setActualPath(TEST_FILE_NAME)
            .setContentMatch(true)
            .build();
        ByteArrayOutputStream serializedMsg = new ByteArrayOutputStream();
        statusMsg.writeDelimitedTo(serializedMsg);
        Mockito.when(mockSatellite.apiCall(anyString(), any()))
            .thenReturn(Flux.just(new ByteArrayInputStream(serializedMsg.toByteArray())));

        ExtFileStatusPojo status = extFilesApiCallHandlerProvider.get()
            .getStatus(TEST_FILE_NAME, TEST_NODE_NAME)
            .contextWrite(contextWrite())
            .block();

        assertThat(status).isNotNull();
        assertThat(status.getActualPath()).isEqualTo(TEST_FILE_NAME);
        assertThat(status.isContentMatch()).isTrue();
    }

    @Test
    public void getStatusUnknownNodeFails() throws Exception
    {
        Throwable thrown = catchThrowable(
            () -> extFilesApiCallHandlerProvider.get()
                .getStatus(TEST_FILE_NAME, "UnknownNode")
                .contextWrite(contextWrite())
                .block()
        );

        assertThat(thrown).isInstanceOf(ApiRcException.class);
        assertThat(((ApiRcException) thrown).getApiCallRc().get(0).getReturnCode())
            .isEqualTo(ApiConsts.FAIL_NOT_FOUND_NODE);
    }

    /*
     * checkFile
     */

    @Test
    public void checkFileAgainstWhitelist() throws Exception
    {
        Set<Path> whitelistedDirs = new HashSet<>();
        whitelistedDirs.add(Paths.get("/etc/allowed"));
        Mockito.when(mockStltConfig.getWhitelistedExternalFilePaths()).thenReturn(whitelistedDirs);
        Mockito.when(mockSatellite.getStltConfig()).thenReturn(mockStltConfig);

        CtrlExternalFilesApiCallHandler handler = extFilesApiCallHandlerProvider.get();
        // parent directory is whitelisted
        assertThat(handler.checkFile("/etc/allowed/my.conf", TEST_NODE_NAME)).isTrue();
        // parent directory not whitelisted
        assertThat(handler.checkFile("/etc/other/my.conf", TEST_NODE_NAME)).isFalse();
        // only the exact parent counts, not grandparents
        assertThat(handler.checkFile("/etc/allowed/sub/my.conf", TEST_NODE_NAME)).isFalse();
        // unknown node
        assertThat(handler.checkFile("/etc/allowed/my.conf", "UnknownNode")).isFalse();
        // invalid (relative) file name
        assertThat(handler.checkFile("relative.conf", TEST_NODE_NAME)).isFalse();
    }

    private class SetExtFileCall extends AbsApiCallTester
    {
        private final String extFileName;
        private final byte[] content;
        private List<String> altSuffixes;

        SetExtFileCall(String extFileNameRef, byte[] contentRef, long... expectedRcs)
        {
            super(
                ApiConsts.MASK_EXT_FILES,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            extFileName = extFileNameRef;
            content = contentRef;
            altSuffixes = null;
        }

        SetExtFileCall setAltSuffixes(String... suffixes)
        {
            altSuffixes = Arrays.asList(suffixes);
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            extFilesApiCallHandlerProvider.get().set(extFileName, content, altSuffixes)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }

    private class DeleteExtFileCall extends AbsApiCallTester
    {
        private final String extFileName;

        DeleteExtFileCall(String extFileNameRef, long... expectedRcs)
        {
            super(
                ApiConsts.MASK_EXT_FILES,
                // the delete api also uses the modify operation context
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            extFileName = extFileNameRef;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            extFilesApiCallHandlerProvider.get().delete(extFileName)
                .contextWrite(contextWrite())
                .toStream()
                .forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
