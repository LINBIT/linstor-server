package com.linbit.linstor.api;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.inject.Key;
import com.linbit.linstor.NetInterface.NetInterfaceApi;
import com.linbit.linstor.Node.NodeType;
import com.linbit.linstor.NodeData;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.annotation.PeerContext;
import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.DoNotSeedDefaultPeer;
import com.linbit.linstor.core.apicallhandler.controller.CtrlNodeApiCallHandler;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessType;
import com.linbit.linstor.security.DummySecurityInitializer;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.security.SecurityLevel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class NodeApiTest extends ApiTestBase
{
    @Inject private Provider<CtrlNodeApiCallHandler> nodeApiCallHandlerProvider;

    private NodeName testNodeName;
    private NodeType testNodeType;
    private NodeData testNode;

    @Mock
    protected Peer mockSatellite;

    public NodeApiTest() throws Exception
    {
        super();
        testNodeName = new NodeName("TestController");
        testNodeType = NodeType.CONTROLLER;
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        testNode = nodeDataFactory.create(
            ApiTestBase.BOB_ACC_CTX,
            testNodeName,
            testNodeType,
            null
        );
        testNode.setPeer(GenericDbBase.SYS_CTX, mockSatellite);
        nodesMap.put(testNodeName, testNode);
        commit();
    }

    @Test
    public void crtSuccess() throws Exception
    {
        evaluateTest(
            new CreateNodeCall(ApiConsts.CREATED)
                .expectStltConnectingAttempt()
        );
    }

    @Test
    public void crtSecondAccDenied() throws Exception
    {
        // FIXME: this test only works because the first API call succeeds.
        // if it would fail, the transaction is currently NOT rolled back.
        evaluateTest(
            new CreateNodeCall(ApiConsts.CREATED)
                .expectStltConnectingAttempt()
        );
        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_EXISTS_NODE)
        );
    }

    @Test
    @DoNotSeedDefaultPeer
    public void crtFailNodesMapViewAccDenied() throws Exception
    {
        DummySecurityInitializer.setSecurityLevel(SYS_CTX, SecurityLevel.MAC);

        nodeRepository.getObjProt().delAclEntry(GenericDbBase.SYS_CTX, GenericDbBase.PUBLIC_CTX.subjectRole);
        nodeRepository.getObjProt().addAclEntry(GenericDbBase.SYS_CTX, GenericDbBase.PUBLIC_CTX.subjectRole, AccessType.VIEW);

        testScope.seed(Key.get(AccessContext.class, PeerContext.class), GenericDbBase.PUBLIC_CTX);
        testScope.seed(Peer.class, mockPeer);
        Mockito.when(mockPeer.getAccessContext()).thenReturn(GenericDbBase.PUBLIC_CTX);

        evaluateTest(
            new CreateNodeCall(ApiConsts.FAIL_ACC_DENIED_NODE)
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
    public void modSuccess() throws Exception
    {
        evaluateTest(
            new ModifyNodeCall(ApiConsts.MODIFIED) // nothing to do
        );
    }

    @Test
    @DoNotSeedDefaultPeer
    public void modDifferentUserAccDenied() throws Exception
    {
        DummySecurityInitializer.setSecurityLevel(SYS_CTX, SecurityLevel.MAC);

        testScope.seed(Key.get(AccessContext.class, PeerContext.class), ApiTestBase.ALICE_ACC_CTX);
        testScope.seed(Peer.class, mockPeer);
        Mockito.when(mockPeer.getAccessContext()).thenReturn(GenericDbBase.PUBLIC_CTX);

        evaluateTest(
            new ModifyNodeCall(ApiConsts.FAIL_ACC_DENIED_NODE)
        );
    }

    private class CreateNodeCall extends AbsApiCallTester
    {
        String nodeName;
        String nodeType;
        List<NetInterfaceApi> netIfApis;
        Map<String, String> props;

        CreateNodeCall(long expectedRc)
        {
            super(
                // peer
                ApiConsts.MASK_NODE,
                ApiConsts.MASK_CRT,
                expectedRc
            );

            nodeName = "TestNode";
            nodeType = ApiConsts.VAL_NODE_TYPE_STLT;
            netIfApis = new ArrayList<>();
            netIfApis.add(
                ApiTestBase.createNetInterfaceApi("tcp0","127.0.0.1")
            );
            props = new TreeMap<>();
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return nodeApiCallHandlerProvider.get().createNode(
                nodeName,
                nodeType,
                netIfApis,
                props
            );
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

        ModifyNodeCall(long retCode)
        {
            super(
                // peer
                ApiConsts.MASK_NODE,
                ApiConsts.MASK_MOD,
                retCode
            );

            nodeUuid = null; // default: do not check against uuid
            nodeName = testNodeName.displayValue;
            nodeType = null; // default: do not update nodeType
            overrideProps = new TreeMap<>();
            deletePropKeys = new TreeSet<>();
            deletePropNamespaces = new TreeSet<>();
        }

        public AbsApiCallTester nodeUuid(java.util.UUID uuid)
        {
            nodeUuid = uuid;
            return this;
        }

        public AbsApiCallTester nodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        public AbsApiCallTester nodeType(String nodeTypeRef)
        {
            nodeType = nodeTypeRef;
            return this;
        }

        public AbsApiCallTester overrideProps(String key, String valueRef)
        {
            overrideProps.put(key, valueRef);
            return this;
        }

        public AbsApiCallTester deleteProp(String key)
        {
            deletePropKeys.add(key);
            return this;
        }

        public AbsApiCallTester deleteNamespace(String namespace)
        {
            deletePropNamespaces.add(namespace);
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return nodeApiCallHandlerProvider.get().modifyNode(
                nodeUuid,
                nodeName,
                nodeType,
                overrideProps,
                deletePropKeys,
                deletePropNamespaces
            );
        }

    }
}
