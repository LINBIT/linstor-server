package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.linstor.core.ClassPathLoader;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorData;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * This class provides Jackson deserializers for {@link LinstorSpec} as well as for {@link DbExportPojoData.Table}
 * instances.
 * The reason for this "combined" deserializer is that during parsing the table's name is stored as currentTable in this
 * class. That reference is used by the LinstorSpec deserializer to tell Jackson to which instance the JSON should be
 * deserialized into.
 */
public class DbExportPojoDataDeserializationHelper
{
    private String currentTable = null;

    private final Map<String, Class<? extends LinstorSpec>> jsonMapping;
    private final LinstorSpecDeserializer linstorSpecDeserializer = new LinstorSpecDeserializer();
    private final TableDeserializer dbExportTableDeserializer = new TableDeserializer();

    /**
     * Initializes the two deserializers as well as search for the correct GenCrd* class, specified by the string
     * parameter
     *
     * @param errorReporterRef
     */
    @SuppressWarnings("unchecked")
    public DbExportPojoDataDeserializationHelper(ErrorReporter errorReporterRef, String genCrdVersionRef)
    {

        ClassPathLoader classPathLoader = new ClassPathLoader(errorReporterRef);
        List<Class<?>> genCrdClasses = classPathLoader.loadClasses(
            GenCrdCurrent.class.getPackage().getName(),
            Collections.singletonList(""),
            null,
            GenCrd.class
        );
        jsonMapping = new HashMap<>();
        for (Class<?> genCrdCls : genCrdClasses)
        {
            GenCrd annot = genCrdCls.getAnnotation(GenCrd.class);
            if (genCrdCls != GenCrdCurrent.class && annot != null && annot.dataVersion().equals(genCrdVersionRef))
            {
                for (Class<?> declaredCls : genCrdCls.getDeclaredClasses())
                {
                    LinstorData linstorDataAnnot = declaredCls.getAnnotation(LinstorData.class);
                    if (linstorDataAnnot != null)
                    {
                        jsonMapping.put(linstorDataAnnot.tableName(), (Class<? extends LinstorSpec>) declaredCls);
                    }
                }
            }
        }
    }

    public TableDeserializer getDbExportTableDeserializer()
    {
        return dbExportTableDeserializer;
    }

    public LinstorSpecDeserializer getLinstorSpecDeserializer()
    {
        return linstorSpecDeserializer;
    }

    private class LinstorSpecDeserializer extends JsonDeserializer<LinstorSpec>
    {
        @Override
        public LinstorSpec deserialize(JsonParser pRef, DeserializationContext ctxtRef)
            throws IOException, JsonProcessingException
        {
            ObjectCodec codec = pRef.getCodec();
            TreeNode readTree = codec.readTree(pRef);

            Class<? extends LinstorSpec> linstorSpecCls = jsonMapping.get(currentTable);

            return codec.treeToValue(readTree, linstorSpecCls);
        }
    }

    private class TableDeserializer extends JsonDeserializer<DbExportPojoData.Table>
    {
        @Override
        public DbExportPojoData.Table deserialize(JsonParser pRef, DeserializationContext ctxtRef)
            throws IOException
        {
            ObjectCodec codec = pRef.getCodec();
            TreeNode readTree = codec.readTree(pRef);

            TreeNode typeTreeNode = readTree.get("name");
            currentTable = ((ValueNode) typeTreeNode).asText();

            // create default deserializer and deserialize without this custom TableDeserializer

            JavaType type = TypeFactory.defaultInstance().constructType(DbExportPojoData.Table.class);
            DeserializationConfig config = ctxtRef.getConfig();
            JsonDeserializer<Object> dfltDbExportTableDeserializer = BeanDeserializerFactory.instance
                .buildBeanDeserializer(
                    ctxtRef,
                    type,
                    config.introspect(type)
                );

            if (dfltDbExportTableDeserializer instanceof ResolvableDeserializer)
            {
                ((ResolvableDeserializer) dfltDbExportTableDeserializer).resolve(ctxtRef);
            }

            JsonParser treeParser = codec.treeAsTokens(readTree);
            config.initialize(treeParser);

            if (treeParser.getCurrentToken() == null)
            {
                treeParser.nextToken();
            }

            return (DbExportPojoData.Table) dfltDbExportTableDeserializer.deserialize(
                treeParser,
                ctxtRef
            );
        }
    }
}
