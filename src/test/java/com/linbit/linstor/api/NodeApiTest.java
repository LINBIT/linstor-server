package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlExecNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeCrtApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.api.rest.v1.serializer.JsonGenTypes;
import com.linbit.linstor.core.apis.NetInterfaceApi;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.proto.requests.MsgReqDrbdReactorExecOuterClass.DrbdReactorCommand;
import com.linbit.linstor.proto.responses.MsgRspDrbdReactorExecOuterClass.MsgRspDrbdReactorExec;
import com.linbit.linstor.utils.externaltools.ExtToolsManager;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.protobuf.ByteString;
import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeApiTest extends ApiTestBase
{
    @Inject private Provider<CtrlExecNodeApiCallHandler> execNodeApiCallHandlerProvider;
    @Inject private Provider<CtrlNodeApiCallHandler> nodeApiCallHandlerProvider;
    @Inject private Provider<CtrlNodeCrtApiCallHandler> nodeCrtApiCallHandlerProvider;

    @Bind @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    private NodeName testNodeName;
    private Node.Type testNodeType;
    private Node testNode;

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected ExtToolsManager mockExtToolsMgr;

    public NodeApiTest() throws Exception
    {
        super();
        testNodeName = new NodeName("TestController");
        testNodeType = Node.Type.CONTROLLER;
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        testNode = nodeFactory.create(
            testNodeName,
            testNodeType,
            null
        );
        testNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testNode);
        leaveScope();

        Mockito.when(mockSatellite.getExtToolsManager()).thenReturn(mockExtToolsMgr);
        Mockito.when(mockSatellite.getConnectionStatus()).thenReturn(ApiConsts.ConnectionStatus.OFFLINE);
        Mockito.when(mockExtToolsMgr.getSupportedLayers()).thenReturn(new TreeSet<>());
        Mockito.when(mockExtToolsMgr.getSupportedProviders()).thenReturn(new TreeSet<>());
    }

    @Test
    public void crtSuccess() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(
                ApiConsts.CREATED,
                ApiConsts.WARN_NOT_CONNECTED
            )
                .expectStltConnectingAttempt(false)
        );
    }

    @Test
    public void crtSecondExists() throws Exception
    {
        // FIXME: this test only works because the first API call succeeds.
        // if it would fail, the transaction is currently NOT rolled back.
        evaluateTest(
            new CreateNodeCall(
                ApiConsts.CREATED,
                ApiConsts.WARN_NOT_CONNECTED
            )
                .expectStltConnectingAttempt(false)
        );
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_EXISTS_NODE)
        );
    }

    @Test
    public void crtMissingNetcom() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_MISSING_NETCOM)
                .clearNetIfApis()
        );
    }

    @Test
    public void crtInvalidNodeName() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_INVLD_NODE_NAME)
                .setNodeName("Test Node") // blank is not allowed
        );
    }

    @Test
    public void crtInvalidNodeType() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_INVLD_NODE_TYPE)
                .setNodeType("special satellite")
        );
    }

    @Test
    public void crtInvalidNetIfName() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_INVLD_NET_NAME)
                .clearNetIfApis()
                .addNetIfApis("invalid net if name", "127.0.0.1")
        );
    }

    @Test
    public void crtInvalidNetAddr() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_INVLD_NET_ADDR)
                .clearNetIfApis()
                .addNetIfApis("net0", "127.0.0.1.42")
        );
    }

    @Test
    public void ctrInvalidNetAddrV6() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_INVLD_NET_ADDR)
                .clearNetIfApis()
                .addNetIfApis("net0", "0::0::0")
        );
    }

    @Test
    public void crtDuplicateNetIfName() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_EXISTS_NET_IF)
                .addNetIfApis("tcp0", "10.0.0.1") // "tcp0" already exists as default netIf
        );
    }

    @Test
    public void modSuccess() throws Exception
    {
        evaluateTest(
            new ModifyNodeCall(ApiConsts.MODIFIED)
        );
    }

    @Test
    public void modWrongNodeUuid() throws Exception
    {
        evaluateTest(
            new ModifyNodeCall(ApiConsts.FAIL_UUID_NODE)
                .nodeUuid(java.util.UUID.randomUUID())
        );
    }

    @Test
    public void modNonExistingNode() throws Exception
    {
        evaluateTest(
            new ModifyNodeCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .nodeName("UnknownNode")
        );
    }

    @Test
    public void modNodeType() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(
                ApiConsts.CREATED,
                ApiConsts.WARN_NOT_CONNECTED
            )
                .expectStltConnectingAttempt(false)
        );
        Node createdNode = nodesMap.get(new NodeName("TestNode"));
        createdNode.setPeer(mockSatellite);

        evaluateTest(
            new ModifyNodeCall(ApiConsts.MODIFIED)
                .nodeName("TestNode")
                .nodeType(ApiConsts.VAL_NODE_TYPE_CMBD)
        );
        assertThat(createdNode.getNodeType()).isEqualTo(Node.Type.COMBINED);
    }

    @Test
    public void modInvalidNodeTypeChange() throws Exception
    {
        // changing from a non-special to a special node type is not allowed
        evaluateTest(
            new ModifyNodeCall(ApiConsts.FAIL_INVLD_NODE_TYPE)
                .nodeType(ApiConsts.VAL_NODE_TYPE_REMOTE_SPDK)
        );
        assertThat(testNode.getNodeType()).isEqualTo(Node.Type.CONTROLLER);
    }

    @Test
    public void modProps() throws Exception
    {
        String auxKey = ApiConsts.NAMESPC_AUXILIARY + "/test";
        evaluateTest(
            new ModifyNodeCall(
                ApiConsts.MASK_CRT | ApiConsts.CREATED, // props set
                ApiConsts.MODIFIED
            )
                .overrideProps(auxKey, "value")
        );
        assertThat(testNode.getProps().getProp(auxKey)).isEqualTo("value");

        evaluateTest(
            new ModifyNodeCall(
                ApiConsts.MASK_DEL | ApiConsts.DELETED, // props deleted
                ApiConsts.MODIFIED
            )
                .deleteProp(auxKey)
        );
        assertThat(testNode.getProps().getProp(auxKey)).isNull();
    }

    @Test
    public void modInvalidProp() throws Exception
    {
        evaluateTest(
            new ModifyNodeCall(ApiConsts.FAIL_INVLD_PROP)
                .overrideProps("ThisIsNotAWhitelistedKey", "value")
        );
    }

    @Test
    public void execDrbdReactorEmptyResponseIsFailure() throws Exception
    {
        Mockito.when(mockSatellite.isOnline()).thenReturn(true);
        Mockito.when(mockSatellite.apiCall(Mockito.anyString(), Mockito.any())).thenReturn(Flux.empty());

        List<JsonGenTypes.ReactorExecResponse> responses = execNodeApiCallHandlerProvider.get()
            .nodeExecDrbdReactor(List.of(testNodeName.displayValue), DrbdReactorCommand.STATUS, null, false)
            .collectList()
            .contextWrite(Context.of(ApiModule.API_CALL_NAME, "test"))
            .block();

        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).node).isEqualTo(testNodeName.displayValue);
        assertThat(responses.get(0).exit_code).isEqualTo(-1);
        assertThat(responses.get(0).stderr_utf8).contains("No response received");
    }

    @Test
    public void execDrbdReactorPeerErrorReturnsPerNodeFailure() throws Exception
    {
        Mockito.when(mockSatellite.isOnline()).thenReturn(true);
        Mockito.when(mockSatellite.apiCall(Mockito.anyString(), Mockito.any()))
            .thenReturn(Flux.error(new RuntimeException("boom")));

        List<JsonGenTypes.ReactorExecResponse> responses = execNodeApiCallHandlerProvider.get()
            .nodeExecDrbdReactor(List.of(testNodeName.displayValue), DrbdReactorCommand.STATUS, null, false)
            .collectList()
            .contextWrite(Context.of(ApiModule.API_CALL_NAME, "test"))
            .block();

        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).node).isEqualTo(testNodeName.displayValue);
        assertThat(responses.get(0).exit_code).isEqualTo(-1);
        assertThat(responses.get(0).stderr_utf8).contains("Communication error: boom");
    }

    @Test
    public void execDrbdReactorResponseContainsUtf8Aliases() throws Exception
    {
        Mockito.when(mockSatellite.isOnline()).thenReturn(true);

        MsgRspDrbdReactorExec protoResp = MsgRspDrbdReactorExec.newBuilder()
            .setExitCode(0)
            .setStdout(ByteString.copyFromUtf8("stdout-value"))
            .setStderr(ByteString.copyFromUtf8("stderr-value"))
            .build();
        ByteArrayOutputStream serializedResp = new ByteArrayOutputStream();
        protoResp.writeDelimitedTo(serializedResp);
        Mockito.when(mockSatellite.apiCall(Mockito.anyString(), Mockito.any()))
            .thenReturn(Flux.just(new ByteArrayInputStream(serializedResp.toByteArray())));

        List<JsonGenTypes.ReactorExecResponse> responses = execNodeApiCallHandlerProvider.get()
            .nodeExecDrbdReactor(List.of(testNodeName.displayValue), DrbdReactorCommand.STATUS, null, false)
            .collectList()
            .contextWrite(Context.of(ApiModule.API_CALL_NAME, "test"))
            .block();

        assertThat(responses).hasSize(1);
        assertThat(responses.get(0).stdout_utf8).isEqualTo("stdout-value");
        assertThat(responses.get(0).stderr_utf8).isEqualTo("stderr-value");
    }

    private class CreateNodeCall extends AbsApiCallTester
    {
        String nodeName;
        String nodeType;
        List<NetInterfaceApi> netIfApis;
        Map<String, String> props;

        CreateNodeCall(long... expectedRcs)
        {
            super(
                // peer
                ApiConsts.MASK_NODE,
                ApiConsts.MASK_CRT,
                expectedRcs
            );

            nodeName = "TestNode";
            nodeType = ApiConsts.VAL_NODE_TYPE_STLT;
            netIfApis = new ArrayList<>();
            netIfApis.add(
                ApiTestBase.createNetInterfaceApi("tcp0", "127.0.0.1")
            );
            props = new TreeMap<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            nodeCrtApiCallHandlerProvider.get().createNode(
                nodeName,
                nodeType,
                netIfApis,
                props
            )
            .contextWrite(contextWrite())
            .toStream().forEach(apiCallRc::addEntries);
            return apiCallRc;
        }

        public AbsApiCallTester setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public AbsApiCallTester setNodeType(String nodeTypeRef)
        {
            nodeType = nodeTypeRef;
            return this;
        }

        public CreateNodeCall clearNetIfApis()
        {
            this.netIfApis.clear();
            return this;
        }

        public CreateNodeCall addNetIfApis(String name, String address)
        {
            this.netIfApis.add(ApiTestBase.createNetInterfaceApi(name, address));
            return this;
        }

        public AbsApiCallTester clearProps()
        {
            this.props.clear();
            return this;
        }

        public AbsApiCallTester setProps(String key, String value)
        {
            this.props.put(key, value);
            return this;
        }
    }

    private class ModifyNodeCall extends AbsApiCallTester
    {

        private java.util.UUID nodeUuid;
        private String nodeName;
        private String nodeType;
        private Map<String, String> overrideProps;
        private Set<String> deletePropKeys;
        private Set<String> deletePropNamespaces;

        ModifyNodeCall(long... expectedRcs)
        {
            super(
                // peer
                ApiConsts.MASK_NODE,
                ApiConsts.MASK_MOD,
                expectedRcs
            );

            nodeUuid = null; // default: do not check against uuid
            nodeName = testNodeName.displayValue;
            nodeType = null; // default: do not update nodeType
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deletePropNamespaces = new TreeSet<>();
        }

        public ModifyNodeCall nodeUuid(java.util.UUID uuid)
        {
            nodeUuid = uuid;
            return this;
        }

        public ModifyNodeCall nodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public ModifyNodeCall nodeType(String nodeTypeRef)
        {
            nodeType = nodeTypeRef;
            return this;
        }

        public ModifyNodeCall overrideProps(String key, String valueRef)
        {
            overrideProps.put(key, valueRef);
            return this;
        }

        public ModifyNodeCall deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        public ModifyNodeCall deleteNamespace(String namespace)
        {
            deletePropNamespaces.add(namespace);
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
            nodeApiCallHandlerProvider.get().modify(
                nodeUuid,
                nodeName,
                nodeType,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            )
            .contextWrite(contextWrite())
            .toStream().forEach(apiCallRc::addEntries);
            return apiCallRc;
        }
    }
}
