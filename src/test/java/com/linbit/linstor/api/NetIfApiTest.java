package com.linbit.linstor.api;

import com.linbit.linstor.api.utils.AbsApiCallTester;
import com.linbit.linstor.core.ApiTestBase;
import com.linbit.linstor.core.apicallhandler.controller.CtrlApiCallHandler;
import com.linbit.linstor.core.apicallhandler.controller.FreeCapacityFetcher;
import com.linbit.linstor.core.identifier.NetInterfaceName;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.objects.NetInterface;
import com.linbit.linstor.core.objects.NetInterface.EncryptionType;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.types.LsIpAddress;
import com.linbit.linstor.core.types.TcpPortNumber;
import com.linbit.linstor.event.EventSerializerDescriptor;
import com.linbit.linstor.event.WatchStore;
import com.linbit.linstor.event.serializer.EventSerializer;
import com.linbit.linstor.netcom.Peer;

import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.util.Collections;
import java.util.Map;

import com.google.inject.testing.fieldbinder.Bind;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

@SuppressWarnings("checkstyle:magicnumber")
public class NetIfApiTest extends ApiTestBase
{
    private static final String TEST_NODE_NAME = "TestSatellite";
    private static final String SECOND_NODE_NAME = "OtherSatellite";
    private static final String ACTIVE_NET_IF = "netif0";

    @Inject
    private Provider<CtrlApiCallHandler> ctrlApiCallHandlerProvider;

    @Bind
    @Mock
    protected FreeCapacityFetcher freeCapacityFetcher;

    // dependencies of the CtrlApiCallHandler facade that no test module provides
    @Bind
    @Mock
    protected WatchStore watchStore;

    @Bind
    protected Map<String, EventSerializer> eventSerializers = Collections.emptyMap();

    @Bind
    protected Map<String, EventSerializerDescriptor> eventSerializerDescriptors = Collections.emptyMap();

    @Mock
    protected Peer mockSatellite;

    @Mock
    protected Peer mockSatellite2;

    private final NodeName testNodeName;
    private final NodeName secondNodeName;

    private Node testNode;
    private NetInterface activeNetIf;

    public NetIfApiTest() throws Exception
    {
        testNodeName = new NodeName(TEST_NODE_NAME);
        secondNodeName = new NodeName(SECOND_NODE_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();

        Mockito.when(mockSatellite.isOnline()).thenReturn(false);
        Mockito.when(mockSatellite.getConnectionStatus()).thenReturn(ApiConsts.ConnectionStatus.OFFLINE);
        Mockito.when(mockSatellite2.isOnline()).thenReturn(false);
        Mockito.when(mockSatellite2.getConnectionStatus()).thenReturn(ApiConsts.ConnectionStatus.OFFLINE);

        testNode = nodeFactory.create(
            testNodeName,
            Node.Type.SATELLITE,
            null
        );
        testNode.setPeer(mockSatellite);
        nodesMap.put(testNodeName, testNode);

        activeNetIf = netInterfaceFactory.create(
            testNode,
            new NetInterfaceName(ACTIVE_NET_IF),
            new LsIpAddress("10.0.0.1"),
            new TcpPortNumber(ApiConsts.DFLT_STLT_PORT_PLAIN),
            EncryptionType.PLAIN
        );
        testNode.setActiveStltConn(activeNetIf);

        leaveScope();
    }

    /*
     * create tests (synchronous api, needs the testScope)
     */

    @Test
    public void createPlainNetIf() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.CREATED)
        );

        NetInterface netIf = testNode.getNetInterface(new NetInterfaceName("netif1"));
        assertThat(netIf).isNotNull();
        assertThat(netIf.getAddress().getAddress()).isEqualTo("10.0.0.2");
        assertThat(netIf.isUsableAsStltConn()).isFalse();
        // the active satellite connection is untouched
        assertThat(testNode.getActiveStltConn()).isSameAs(activeNetIf);
        Mockito.verify(satelliteConnector, Mockito.never()).startConnecting(any(Node.class));
    }

    @Test
    public void createStltConnNetIfKeepsCurrentActiveConn() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.CREATED)
                .setStltConn(3367, ApiConsts.VAL_NETCOM_TYPE_PLAIN)
        );

        NetInterface netIf = testNode.getNetInterface(new NetInterfaceName("netif1"));
        assertThat(netIf.isUsableAsStltConn()).isTrue();
        assertThat(testNode.getActiveStltConn()).isSameAs(activeNetIf);
        Mockito.verify(satelliteConnector, Mockito.never()).startConnecting(any(Node.class));
    }

    @Test
    public void createSetActiveSwitchesActiveConn() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.CREATED)
                .setStltConn(3367, ApiConsts.VAL_NETCOM_TYPE_PLAIN)
                .setActive(true)
        );

        assertThat(testNode.getActiveStltConn().getName().displayValue).isEqualTo("netif1");
        Mockito.verify(mockSatellite).closeConnection(false);
        Mockito.verify(satelliteConnector).startConnecting(any(Node.class));
    }

    @Test
    public void createFirstStltConnNetIfBecomesActive() throws Exception
    {
        enterScope();
        Node secondNode = createSecondNode();

        evaluateTest(
            new CreateNetIfCall(ApiConsts.CREATED)
                .setNodeName(SECOND_NODE_NAME)
                .setStltConn(3367, ApiConsts.VAL_NETCOM_TYPE_PLAIN)
        );

        assertThat(secondNode.getActiveStltConn()).isNotNull();
        assertThat(secondNode.getActiveStltConn().getName().displayValue).isEqualTo("netif1");
        Mockito.verify(satelliteConnector).startConnecting(any(Node.class));
    }

    @Test
    public void createPlainNetIfWithoutStltConnWarns() throws Exception
    {
        enterScope();
        Node secondNode = createSecondNode();

        // characterization: the netIf is created and committed, but the response only contains
        // the warning that the node still has no active satellite connection
        evaluateTest(
            new CreateNetIfCall(ApiConsts.WARN_NO_STLT_CONN_DEFINED)
                .setNodeName(SECOND_NODE_NAME)
        );

        assertThat(secondNode.getNetInterface(new NetInterfaceName("netif1"))).isNotNull();
        assertThat(secondNode.getActiveStltConn()).isNull();
    }

    @Test
    public void createSetActiveWithoutStltPortFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.FAIL_INVLD_NET_PORT | ApiConsts.FAIL_INVLD_ENCRYPT_TYPE)
                .setActive(true)
        );

        assertThat(testNode.getActiveStltConn()).isSameAs(activeNetIf);
    }

    @Test
    public void createExistingNetIfFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.FAIL_EXISTS_NET_IF)
                .setNetIfName(ACTIVE_NET_IF)
        );
    }

    @Test
    public void createInvalidNetIfNameFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.FAIL_INVLD_NET_NAME)
                .setNetIfName("invalid name") // blank is not allowed
        );
    }

    @Test
    public void createInvalidAddressFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.FAIL_INVLD_NET_ADDR)
                .setAddress("10.0.0.1.42")
        );
    }

    @Test
    public void createInvalidEncrTypeFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.FAIL_INVLD_NET_TYPE)
                .setStltConn(3367, "bogus")
        );
    }

    @Test
    public void createOnUnknownNodeFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new CreateNetIfCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void createOnSpecialNodeFails() throws Exception
    {
        enterScope();
        NodeName specialNodeName = new NodeName("SpecialNode");
        Node specialNode = nodeFactory.create(
            specialNodeName,
            Node.Type.REMOTE_SPDK,
            null
        );
        nodesMap.put(specialNodeName, specialNode);

        evaluateTest(
            new CreateNetIfCall(ApiConsts.FAIL_INVLD_NODE_TYPE)
                .setNodeName("SpecialNode")
        );
    }

    /*
     * modify tests
     */

    @Test
    public void modifyAddressOfActiveConnReconnects() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyNetIfCall(ApiConsts.MODIFIED)
                .setAddress("10.0.0.99")
        );

        assertThat(activeNetIf.getAddress().getAddress()).isEqualTo("10.0.0.99");
        Mockito.verify(mockSatellite).closeConnection();
        Mockito.verify(satelliteConnector).startConnecting(any(Node.class));
    }

    @Test
    public void modifyAddressOfNonActiveNetIfDoesNotReconnect() throws Exception
    {
        enterScope();
        createPlainNetIf(testNode, "netif1", "10.0.0.2");

        evaluateTest(
            new ModifyNetIfCall(ApiConsts.MODIFIED)
                .setNetIfName("netif1")
                .setAddress("10.0.0.3")
        );

        NetInterface netIf = testNode.getNetInterface(new NetInterfaceName("netif1"));
        assertThat(netIf.getAddress().getAddress()).isEqualTo("10.0.0.3");
        Mockito.verify(satelliteConnector, Mockito.never()).startConnecting(any(Node.class));
    }

    @Test
    public void modifyStltPortAndEncrType() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyNetIfCall(ApiConsts.MODIFIED)
                .setStltConn(3367, ApiConsts.VAL_NETCOM_TYPE_SSL)
        );

        assertThat(activeNetIf.getStltConnPort().value).isEqualTo(3367);
        assertThat(activeNetIf.getStltConnEncryptionType()).isEqualTo(EncryptionType.SSL);
        // port / encryption type of the active satellite connection changed -> reconnect
        Mockito.verify(satelliteConnector).startConnecting(any(Node.class));
    }

    @Test
    public void modifySetActiveSwitchesActiveConn() throws Exception
    {
        enterScope();
        createStltConnNetIf(testNode, "netif1", "10.0.0.2", 3367);

        evaluateTest(
            new ModifyNetIfCall(ApiConsts.MODIFIED)
                .setNetIfName("netif1")
                .setActive(true)
        );

        assertThat(testNode.getActiveStltConn().getName().displayValue).isEqualTo("netif1");
        Mockito.verify(satelliteConnector).startConnecting(any(Node.class));
    }

    @Test
    public void modifySetActiveOnPlainNetIfFails() throws Exception
    {
        enterScope();
        createPlainNetIf(testNode, "netif1", "10.0.0.2");

        evaluateTest(
            new ModifyNetIfCall(ApiConsts.FAIL_INVLD_NET_PORT | ApiConsts.FAIL_INVLD_ENCRYPT_TYPE)
                .setNetIfName("netif1")
                .setActive(true)
        );

        assertThat(testNode.getActiveStltConn()).isSameAs(activeNetIf);
    }

    @Test
    public void modifyUnknownNetIfFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyNetIfCall(ApiConsts.FAIL_NOT_FOUND_NET_IF)
                .setNetIfName("unknown")
                .setAddress("10.0.0.3")
        );
    }

    @Test
    public void modifyOnUnknownNodeFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new ModifyNetIfCall(ApiConsts.FAIL_NOT_FOUND_NODE)
                .setNodeName("UnknownNode")
                .setAddress("10.0.0.3")
        );
    }

    @Test
    public void modifyOnNodeWithoutActiveStltConn() throws Exception
    {
        enterScope();
        Node secondNode = createSecondNode();
        createPlainNetIf(secondNode, "netif1", "10.0.1.2");

        // the address is modified and committed, but since the node still has no active satellite
        // connection the response only carries the WARN_NO_STLT_CONN_DEFINED entry (same as
        // createNetIf / deleteNetIf)
        evaluateTest(
            new ModifyNetIfCall(ApiConsts.WARN_NO_STLT_CONN_DEFINED)
                .setNodeName(SECOND_NODE_NAME)
                .setNetIfName("netif1")
                .setAddress("10.0.1.3")
        );

        NetInterface netIf = secondNode.getNetInterface(new NetInterfaceName("netif1"));
        assertThat(netIf.getAddress().getAddress()).isEqualTo("10.0.1.3");
    }

    /*
     * delete tests
     */

    @Test
    public void deleteNonActiveNetIf() throws Exception
    {
        enterScope();
        createPlainNetIf(testNode, "netif1", "10.0.0.2");

        evaluateTest(
            new DeleteNetIfCall(ApiConsts.DELETED)
                .setNetIfName("netif1")
        );

        assertThat(testNode.getNetInterface(new NetInterfaceName("netif1"))).isNull();
        assertThat(testNode.getActiveStltConn()).isSameAs(activeNetIf);
        Mockito.verify(satelliteConnector, Mockito.never()).startConnecting(any(Node.class));
    }

    @Test
    public void deleteActiveNetIfSwitchesToReplacement() throws Exception
    {
        enterScope();
        createStltConnNetIf(testNode, "netif1", "10.0.0.2", 3367);

        evaluateTest(
            new DeleteNetIfCall(ApiConsts.DELETED)
                .setNetIfName(ACTIVE_NET_IF)
        );

        assertThat(testNode.getNetInterface(new NetInterfaceName(ACTIVE_NET_IF))).isNull();
        assertThat(testNode.getActiveStltConn().getName().displayValue).isEqualTo("netif1");
        Mockito.verify(mockSatellite).closeConnection();
        Mockito.verify(satelliteConnector).startConnecting(any(Node.class));
    }

    @Test
    public void deleteLastStltConnNetIfWarns() throws Exception
    {
        enterScope();
        // characterization: the netIf is deleted, but the DELETED response is replaced by the
        // warning that the node lost its last satellite connection
        evaluateTest(
            new DeleteNetIfCall(ApiConsts.WARN_NO_STLT_CONN_DEFINED)
                .setNetIfName(ACTIVE_NET_IF)
        );

        assertThat(testNode.getNetInterface(new NetInterfaceName(ACTIVE_NET_IF))).isNull();
        assertThat(testNode.getActiveStltConn()).isNull();
        Mockito.verify(mockSatellite).setConnectionStatus(ApiConsts.ConnectionStatus.NO_STLT_CONN);
    }

    @Test
    public void deleteNonexistentNetIfWarns() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteNetIfCall(ApiConsts.WARN_NOT_FOUND)
                .setNetIfName("unknown")
        );
    }

    @Test
    public void deleteOnUnknownNodeWarns() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteNetIfCall(ApiConsts.WARN_NOT_FOUND)
                .setNodeName("UnknownNode")
        );
    }

    @Test
    public void deleteInvalidNetIfNameFails() throws Exception
    {
        enterScope();
        evaluateTest(
            new DeleteNetIfCall(ApiConsts.FAIL_INVLD_NET_NAME)
                .setNetIfName("invalid name")
        );
    }

    private Node createSecondNode() throws Exception
    {
        Node secondNode = nodeFactory.create(
            secondNodeName,
            Node.Type.SATELLITE,
            null
        );
        secondNode.setPeer(mockSatellite2);
        nodesMap.put(secondNodeName, secondNode);
        return secondNode;
    }

    private NetInterface createPlainNetIf(Node node, String netIfName, String address) throws Exception
    {
        return netInterfaceFactory.create(
            node,
            new NetInterfaceName(netIfName),
            new LsIpAddress(address),
            null,
            null
        );
    }

    private NetInterface createStltConnNetIf(Node node, String netIfName, String address, int port) throws Exception
    {
        return netInterfaceFactory.create(
            node,
            new NetInterfaceName(netIfName),
            new LsIpAddress(address),
            new TcpPortNumber(port),
            EncryptionType.PLAIN
        );
    }

    private class CreateNetIfCall extends AbsApiCallTester
    {
        private String nodeName;
        private String netIfName;
        private String address;
        private Integer stltPort;
        private String stltEncrType;
        private Boolean setActive;

        CreateNetIfCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_NET_IF,
                ApiConsts.MASK_CRT,
                expectedRcs
            );
            nodeName = TEST_NODE_NAME;
            netIfName = "netif1";
            address = "10.0.0.2";
            stltPort = null;
            stltEncrType = null;
            setActive = null;
        }

        CreateNetIfCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        CreateNetIfCall setNetIfName(String netIfNameRef)
        {
            netIfName = netIfNameRef;
            return this;
        }

        CreateNetIfCall setAddress(String addressRef)
        {
            address = addressRef;
            return this;
        }

        CreateNetIfCall setStltConn(Integer port, String encrType)
        {
            stltPort = port;
            stltEncrType = encrType;
            return this;
        }

        CreateNetIfCall setActive(Boolean setActiveRef)
        {
            setActive = setActiveRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().createNetInterface(
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType,
                setActive
            );
        }
    }

    private class ModifyNetIfCall extends AbsApiCallTester
    {
        private String nodeName;
        private String netIfName;
        private String address;
        private Integer stltPort;
        private String stltEncrType;
        private Boolean setActive;

        ModifyNetIfCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_NET_IF,
                ApiConsts.MASK_MOD,
                expectedRcs
            );
            nodeName = TEST_NODE_NAME;
            netIfName = ACTIVE_NET_IF;
            address = null;
            stltPort = null;
            stltEncrType = null;
            setActive = null;
        }

        ModifyNetIfCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        ModifyNetIfCall setNetIfName(String netIfNameRef)
        {
            netIfName = netIfNameRef;
            return this;
        }

        ModifyNetIfCall setAddress(String addressRef)
        {
            address = addressRef;
            return this;
        }

        ModifyNetIfCall setStltConn(Integer port, String encrType)
        {
            stltPort = port;
            stltEncrType = encrType;
            return this;
        }

        ModifyNetIfCall setActive(Boolean setActiveRef)
        {
            setActive = setActiveRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().modifyNetInterface(
                nodeName,
                netIfName,
                address,
                stltPort,
                stltEncrType,
                setActive
            );
        }
    }

    private class DeleteNetIfCall extends AbsApiCallTester
    {
        private String nodeName;
        private String netIfName;

        DeleteNetIfCall(long... expectedRcs)
        {
            super(
                ApiConsts.MASK_NET_IF,
                ApiConsts.MASK_DEL,
                expectedRcs
            );
            nodeName = TEST_NODE_NAME;
            netIfName = "netif1";
        }

        DeleteNetIfCall setNodeName(String nodeNameRef)
        {
            nodeName = nodeNameRef;
            return this;
        }

        DeleteNetIfCall setNetIfName(String netIfNameRef)
        {
            netIfName = netIfNameRef;
            return this;
        }

        @Override
        public ApiCallRc executeApiCall()
        {
            return ctrlApiCallHandlerProvider.get().deleteNetInterface(
                nodeName,
                netIfName
            );
        }
    }
}
