package com.scaleunlimited.cascading.lucene;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.metrics.spi.NullContext;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LuceneSchemeTest {

    @SuppressWarnings({ "unchecked", "serial" })
    public static class CalcBoost extends BaseOperation<NullContext> implements Function<NullContext> {

        public CalcBoost() {
            super(LuceneScheme.BOOST_FIELDS);
        }

        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            TupleEntry entry = funcCall.getArguments();
            TupleEntryCollector collector = funcCall.getOutputCollector();

            String value = entry.getString("parsed-content");
            String[] values = value.split(" ");
            float boostValue = Float.parseFloat(values[2]);
            TupleEntry boost = new TupleEntry(LuceneScheme.BOOST_FIELDS, new Tuple(boostValue));
            collector.add(boost);
        }
    }
    
    @Test
    public void testIndexSink() throws Exception {
        String out = "build/test/IndexSchemaTest/testIndexSink/out";

        Lfs lfs = new Lfs(new LuceneScheme( new Fields("text"), 
                                            new Store[] { Store.NO }, 
                                            new Index[] { Index.NOT_ANALYZED }, 
                                            false, 
                                            null,
                                            Integer.MAX_VALUE, 
                                            10), out,
                        SinkMode.REPLACE);
        
        TupleEntryCollector writer = lfs.openForWrite(new HadoopFlowProcess());

        for (int i = 0; i < 100; i++) {
            writer.add(new Tuple("some text"));
        }

        writer.close();
    }

    @Test
    public void testPipeIntoIndex() throws Exception {
        String in = "build/test/IndexSchemaTest/testPipeIntoIndex/in";

        final Fields indexedFields = new Fields("id", "value", "boost");
        Lfs lfs = new Lfs(new SequenceFile(indexedFields), in, SinkMode.REPLACE);
        TupleEntryCollector write = lfs.openForWrite(new HadoopFlowProcess());
        for (int i = 0; i < 10; i++) {
            float boost = 0.5f + i/5.0f;
            write.add(new Tuple(i, "test " + i + " " + boost, boost));
        }
        write.close();

        Store[] storeSettings = new Store[] { Store.YES, Store.YES };
        Index[] indexSettings = new Index[] { Index.NOT_ANALYZED, Index.ANALYZED };
        LuceneScheme scheme = new LuceneScheme(indexedFields, storeSettings, indexSettings, true, null, Integer.MAX_VALUE, 10);

        // Rename the fields from what we've got, to what the Lucene field names should be.
        Pipe indexingPipe = new Pipe("index pipe");
        
        String out = "build/test/IndexSchemaTest/testPipeIntoIndex/out";
        Lfs indexSinkTap = new Lfs(scheme, out, SinkMode.REPLACE);
        
        Flow flow = new HadoopFlowConnector().connect(lfs, indexSinkTap, indexingPipe);
        flow.complete();

        File file = new File(out);
        File[] listFiles = file.listFiles();
        IndexReader[] indexReaders = new IndexReader[listFiles.length];

        for (int i = 0; i < listFiles.length; i++) {
            File indexFile = listFiles[i];
            indexReaders[i] = IndexReader.open(new MMapDirectory(indexFile));
        }

        QueryParser parser = new QueryParser(Version.LUCENE_41, "value", new StandardAnalyzer(Version.LUCENE_41));
        IndexSearcher searcher = new IndexSearcher(new MultiReader(indexReaders));
        for (int i = 0; i < 10; i++) {
            TopDocs search = searcher.search(parser.parse("" + i), 1);
            
            assertEquals(1, search.totalHits);
        }
        
        // the top hit should be the last document we added, which will have
        // the max boost.
        TopDocs hits = searcher.search(parser.parse("test"), 1);
        assertEquals(10, hits.totalHits);
        ScoreDoc[] docs = hits.scoreDocs;
        Document doc = searcher.doc(docs[0].doc);
        String content = doc.get("value");
        assertTrue(content.startsWith("test 9"));
    }
}
