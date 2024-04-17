package com.linbit.linstor.core.apicallhandler.controller.autoplacer;

import com.linbit.linstor.api.interfaces.AutoSelectFilterApi;
import com.linbit.linstor.api.pojo.builder.AutoSelectFilterBuilder;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.StorPoolName;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.DummySecurityInitializer;
import com.linbit.linstor.security.GenericDbBase;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SelectionManagerTest extends GenericDbBase
{
    private static final String zoneKey = "Aux/zone";
    private static final String rackKey = "Aux/rack";
    private Autoplacer.StorPoolWithScore[] storPoolWithScores;
    private HashMap<String, Node> nodes;

    private AccessContext accessContext;

    @Before
    public void setUp() throws Exception
    {
        super.setUpAndEnterScope();
        accessContext = DummySecurityInitializer.getSystemAccessContext();
        nodes = new HashMap<>();

        StorPoolDefinition dfltDisklessPoolDef = storPoolDefinitionRepository
            .get(accessContext, new StorPoolName(LinStor.DISKLESS_STOR_POOL_NAME));
        StorPoolDefinition diskfulPoolDef = storPoolDefinitionFactory
            .create(accessContext, new StorPoolName("pool1"));

        ArrayList<Autoplacer.StorPoolWithScore> storPools = new ArrayList<>(24);
        // We create 12 nodes:
        // * 3 zones: a, b, c
        // * 2 racks: 1, 2
        // * 2 nodes per rack: 1, 2
        // node name will be: node-<zone><rack>-<nr>
        // i.e. node-b1-2 is a node in zone b, first rack, second node in rack
        for (String zone : Arrays.asList("a", "b", "c"))
        {
            for (int rack = 0; rack < 2; rack++)
            {
                for (int i = 0; i < 2; i++)
                {
                    String nodeName = String.format("node-%s%d-%d", zone, rack + 1, i + 1);
                    Node node = nodeFactory.create(
                        accessContext,
                        new NodeName(nodeName),
                        Node.Type.SATELLITE,
                        new Node.Flags[0]);
                    node.getProps(accessContext).setProp(zoneKey, zone);
                    node.getProps(accessContext).setProp(rackKey, Integer.toString(rack + 1));

                    nodes.put(nodeName, node);

                    storPools.add(new Autoplacer.StorPoolWithScore(
                        storPoolFactory.create(
                            accessContext,
                            node,
                            dfltDisklessPoolDef,
                            DeviceProviderKind.DISKLESS,
                            getFreeSpaceMgr(dfltDisklessPoolDef, node),
                            false
                        ),
                        0
                    ));
                    storPools.add(new Autoplacer.StorPoolWithScore(
                        storPoolFactory.create(
                            accessContext,
                            node,
                            diskfulPoolDef,
                            DeviceProviderKind.LVM_THIN,
                            getFreeSpaceMgr(diskfulPoolDef, node),
                            false
                        ),
                        1.0
                    ));
                }
            }
        }

        storPoolWithScores = storPools.toArray(new Autoplacer.StorPoolWithScore[0]);
        Arrays.sort(storPoolWithScores);
    }

    @Test
    public void unconstrainedSelection() throws Exception
    {
        AutoSelectFilterApi selectFilter = new AutoSelectFilterBuilder()
            .setPlaceCount(3)
            .build();

        SelectionManager selectionManager = new SelectionManager(
            DummySecurityInitializer.getSystemAccessContext(),
            errorReporter,
            selectFilter,
            Collections.emptyList(),
            0,
            0,
            Collections.emptyList(),
            Collections.emptyMap(),
            storPoolWithScores,
            false
        );

        Set<Autoplacer.StorPoolWithScore> actual = selectionManager.findSelection(0);
        Assert.assertEquals(3, actual.size());
        // In this case (no constraints), the selection should just take the first 3 pools.
        Assert.assertTrue(actual.containsAll(Arrays.asList(
            storPoolWithScores[0],
            storPoolWithScores[1],
            storPoolWithScores[2])));
    }

    @Test
    public void constrainedSelection() throws Exception
    {
        AutoSelectFilterApi selectFilter = new AutoSelectFilterBuilder()
            .setPlaceCount(3)
            .setReplicasOnSameList(Collections.singletonList(rackKey))
            .setReplicasOnDifferentList(Collections.singletonList(zoneKey))
            .build();

        SelectionManager selectionManager = new SelectionManager(
            DummySecurityInitializer.getSystemAccessContext(),
            errorReporter,
            selectFilter,
            Collections.emptyList(),
            0,
            0,
            Collections.emptyList(),
            Collections.emptyMap(),
            storPoolWithScores,
            false
        );

        Set<Autoplacer.StorPoolWithScore> actual = selectionManager.findSelection(0);
        Assert.assertEquals(3, actual.size());

        String expectedKey = storPoolWithScores[0].storPool.getNode().getProps(accessContext).getProp(rackKey);
        Set<String> seenZones = new HashSet<>();

        for (Autoplacer.StorPoolWithScore pool : actual)
        {
            Assert.assertEquals(expectedKey, pool.storPool.getNode().getProps(accessContext).getProp(rackKey));
            Assert.assertTrue(seenZones.add(pool.storPool.getNode().getProps(accessContext).getProp(zoneKey)));
        }
    }

    @Test
    public void constrainedSelectionNoSolution() throws Exception
    {
        AutoSelectFilterApi selectFilter = new AutoSelectFilterBuilder()
            .setPlaceCount(3)
            // There are only 2 nodes in a zone/rack combination
            .setReplicasOnSameList(Arrays.asList(zoneKey, rackKey))
            .build();

        SelectionManager selectionManager = new SelectionManager(
            DummySecurityInitializer.getSystemAccessContext(),
            errorReporter,
            selectFilter,
            Collections.emptyList(),
            0,
            0,
            Collections.emptyList(),
            Collections.emptyMap(),
            storPoolWithScores,
            false
        );

        Set<Autoplacer.StorPoolWithScore> actual = selectionManager.findSelection(0);
        Assert.assertEquals(0, actual.size());
    }

    @Test
    public void partialDiffSelection() throws Exception
    {
        AutoSelectFilterApi selectFilter = new AutoSelectFilterBuilder()
            .setPlaceCount(3)
            .setReplicasOnDifferentList(Collections.singletonList(zoneKey))
            .build();

        SelectionManager selectionManager = new SelectionManager(
            DummySecurityInitializer.getSystemAccessContext(),
            errorReporter,
            selectFilter,
            Arrays.asList(nodes.get("node-a1-1"), nodes.get("node-b1-2")),
            2,
            0,
            Collections.emptyList(),
            Collections.emptyMap(),
            storPoolWithScores,
            false
        );

        Set<Autoplacer.StorPoolWithScore> actual = selectionManager.findSelection(0);
        Assert.assertEquals(1, actual.size());
        for (Autoplacer.StorPoolWithScore selected : actual)
        {
            Assert.assertEquals("c", selected.storPool.getNode().getProps(accessContext).getProp(zoneKey));
        }
    }

    @Test
    public void partialSameSelection() throws Exception
    {
        AutoSelectFilterApi selectFilter = new AutoSelectFilterBuilder()
            .setPlaceCount(3)
            .setReplicasOnSameList(Collections.singletonList(zoneKey))
            .build();

        SelectionManager selectionManager = new SelectionManager(
            DummySecurityInitializer.getSystemAccessContext(),
            errorReporter,
            selectFilter,
            Arrays.asList(nodes.get("node-c1-1"), nodes.get("node-c1-2")),
            2,
            0,
            Collections.emptyList(),
            Collections.emptyMap(),
            storPoolWithScores,
            false
        );

        Set<Autoplacer.StorPoolWithScore> actual = selectionManager.findSelection(0);
        Assert.assertEquals(1, actual.size());
        for (Autoplacer.StorPoolWithScore selected : actual)
        {
            Assert.assertEquals("c", selected.storPool.getNode().getProps(accessContext).getProp(zoneKey));
        }
    }

    @Test
    public void fixedProperties() throws Exception
    {
        // Normally this selection is done in the prefilter. But for TieBreaker selection, we
        // might want to filter it again here.
        AutoSelectFilterApi selectFilter = new AutoSelectFilterBuilder()
            .setPlaceCount(3)
            .setReplicasOnSameList(Collections.singletonList(rackKey + "=2"))
            .setReplicasOnDifferentList(Collections.singletonList(zoneKey))
            .build();

        SelectionManager selectionManager = new SelectionManager(
            DummySecurityInitializer.getSystemAccessContext(),
            errorReporter,
            selectFilter,
            Collections.emptyList(),
            0,
            0,
            Collections.emptyList(),
            Collections.emptyMap(),
            storPoolWithScores,
            false
        );

        Set<Autoplacer.StorPoolWithScore> actual = selectionManager.findSelection(0);
        Assert.assertEquals(3, actual.size());

        Set<String> seenZones = new HashSet<>();

        for (Autoplacer.StorPoolWithScore pool : actual)
        {
            Assert.assertEquals("2", pool.storPool.getNode().getProps(accessContext).getProp(rackKey));
            Assert.assertTrue(seenZones.add(pool.storPool.getNode().getProps(accessContext).getProp(zoneKey)));
        }
    }
}
