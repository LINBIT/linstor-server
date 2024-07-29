package com.linbit.linstor.core.apicallhandler.controller.db;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.ClassPathLoader;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.GenCrdCurrent;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorCrd;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorData;
import com.linbit.linstor.dbdrivers.k8s.crd.LinstorSpec;
import com.linbit.linstor.logging.ErrorReporter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.node.ValueNode;

/**
 * This class provides Jackson deserializers for {@link LinstorSpec} as well as for {@link DbExportPojoData.Table}
 * instances.
 * The reason for this "combined" deserializer is that during parsing the table's name is stored as currentTable in this
 * class. That reference is used by the LinstorSpec deserializer to tell Jackson to which instance the JSON should be
 * deserialized into.
 */
public class DbExportPojoDataDeserializationHelper
{
    private @Nullable String currentTable = null;

    private final Map<String, Class<? extends LinstorSpec<?, ?>>> jsonLinstorSpecMapping;
    private final Map<String, Class<? extends LinstorCrd<?>>> jsonLinstorCrdMapping;
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
        jsonLinstorSpecMapping = new HashMap<>();
        jsonLinstorCrdMapping = new HashMap<>();
        for (Class<?> genCrdCls : genCrdClasses)
        {
            GenCrd annot = genCrdCls.getAnnotation(GenCrd.class);
            if (genCrdCls != GenCrdCurrent.class && annot != null && annot.dataVersion().equals(genCrdVersionRef))
            {
                HashMap<String, String> expectedLowerCaseCrdClassNamesToDbTblName = new HashMap<>();

                for (Class<?> declaredLinstorSpecCls : genCrdCls.getDeclaredClasses())
                {
                    LinstorData linstorDataAnnot = declaredLinstorSpecCls.getAnnotation(LinstorData.class);
                    if (linstorDataAnnot != null)
                    {
                        jsonLinstorSpecMapping.put(
                            linstorDataAnnot.tableName(),
                            (Class<? extends LinstorSpec<?, ?>>) declaredLinstorSpecCls
                        );

                        String simpleName = declaredLinstorSpecCls.getSimpleName();
                        expectedLowerCaseCrdClassNamesToDbTblName.put(
                            simpleName.substring(0, simpleName.length() - "Spec".length()).toLowerCase(),
                            linstorDataAnnot.tableName()
                        );
                    }
                }

                for (Class<?> declaredLinstorCrdCls : genCrdCls.getDeclaredClasses())
                {
                    String dbTblName = expectedLowerCaseCrdClassNamesToDbTblName.get(
                        declaredLinstorCrdCls.getSimpleName().toLowerCase()
                    );
                    if (dbTblName != null)
                    {
                        jsonLinstorCrdMapping.put(
                            dbTblName,
                            (Class<? extends LinstorCrd<?>>) declaredLinstorCrdCls
                        );
                    }
                }

                break;
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

    private class LinstorSpecDeserializer extends JsonDeserializer<LinstorSpec<?, ?>>
    {
        @Override
        public LinstorSpec<?, ?> deserialize(JsonParser pRef, DeserializationContext ctxtRef)
            throws IOException
        {
            ObjectCodec codec = pRef.getCodec();
            TreeNode readTree = codec.readTree(pRef);

            Class<? extends LinstorSpec<?, ?>> linstorSpecCls = jsonLinstorSpecMapping.get(currentTable);

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

            TreeNode typeTreeNode = readTree.get(DbExportPojoData.Table.JSON_CLM_NAME);
            currentTable = ((ValueNode) typeTreeNode).asText();

            TreeNode clmDscrSubTree = readTree.get(DbExportPojoData.Table.JSON_CLM_CLM_DESCR);
            JsonParser clmDscrJsonParser = clmDscrSubTree.traverse();
            clmDscrJsonParser.setCodec(codec);
            List<DbExportPojoData.Column> clmDscrList = clmDscrJsonParser.readValueAs(
                new TypeReference<List<DbExportPojoData.Column>>()
            {
            });

            TreeNode dataSubTree = readTree.get(DbExportPojoData.Table.JSON_CLM_DATA);
            JsonParser dataJsonParser = dataSubTree.traverse();
            dataJsonParser.setCodec(codec);
            List<LinstorSpec<?, ?>> dataList = dataJsonParser.readValueAs(
                new TypeReference<List<LinstorSpec<?, ?>>>()
                {
                }
            );

            return new DbExportPojoData.Table(
                currentTable,
                clmDscrList,
                dataList,
                jsonLinstorCrdMapping.get(currentTable)
            );
        }
    }
}
