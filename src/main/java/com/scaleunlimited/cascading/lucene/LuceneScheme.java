package com.scaleunlimited.cascading.lucene;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.util.Version;
import org.xml.sax.SAXException;

import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings({ "serial", "deprecation" })
public class LuceneScheme extends Scheme<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>, Object[], Void> {
    private static final Logger LOGGER = Logger.getLogger(LuceneScheme.class);
    
    public static final String BOOST_FIELD = LuceneOutputFormat.class.getSimpleName() + "_" + "boost";
    public static final Fields BOOST_FIELDS = new Fields(BOOST_FIELD);

    private int _maxSegments;
    private Class<? extends Analyzer> _analyzer;
    private int _maxFieldLength;
    private Store[] _storeSettings;
    private Index[] _indexSettings;
    private boolean _hasBoost;

    public static class DefaultAnalyzer extends Analyzer {

        private Analyzer _analyzer;
        
        public DefaultAnalyzer() {
            _analyzer = new StandardAnalyzer(Version.LUCENE_40);
        }
        
        @Override
        public TokenStream tokenStream(String fieldName, Reader reader) {
            // TODO Auto-generated method stub
            return _analyzer.tokenStream(fieldName, reader);
        }
    }
    
    public LuceneScheme(Fields schemeFields) throws IOException, ParserConfigurationException, SAXException {
        this(schemeFields, null, null, false, DefaultAnalyzer.class, Integer.MAX_VALUE, LuceneOutputFormat.DEFAULT_MAX_SEGMENTS);
    }
    
    public LuceneScheme(Fields schemeFields, Store[] storeSettings, Index[] indexSettings, boolean hasBoost, Class<? extends Analyzer> analyzer, int maxFieldLength, int maxSegments) throws IOException, ParserConfigurationException, SAXException {
        super(schemeFields, schemeFields);
        
        _hasBoost = hasBoost;
        _analyzer = analyzer;
        _maxFieldLength = maxFieldLength;
        _maxSegments = maxSegments;
        
        int numFields = schemeFields.size();
        if (hasBoost) {
            numFields -= 1;
        }
        
        if (numFields == 0) {
            throw new IllegalArgumentException("At least one field must be specified for indexing");
        }
        
        if (storeSettings == null) {
            _storeSettings = new Store[numFields];
            Arrays.fill(_storeSettings, Store.YES);
        } else if (storeSettings.length != numFields) {
            throw new IllegalArgumentException("storeSettings[] needs to have same length as schemeFields");
        } else {
            _storeSettings = storeSettings;
        }
        
        if (indexSettings == null) {
            _indexSettings = new Index[numFields];
            Arrays.fill(_indexSettings, Index.ANALYZED);
        } else if (indexSettings.length != numFields) {
            throw new IllegalArgumentException("indexSettings[] needs to have same length as schemeFields");
        } else {
            _indexSettings = indexSettings;
        }
    }
    
    @Override
    public void sourceConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        throw new TapException("LuceneScheme can only be used as a sink, not a source");
    }

    @Override
    public void sinkConfInit(FlowProcess<JobConf> flowProcess, Tap<JobConf, RecordReader<Tuple, Tuple>, OutputCollector<Tuple, Tuple>> tap, JobConf conf) {
        conf.setOutputKeyClass(Tuple.class);
        conf.setOutputValueClass(Tuple.class);
        conf.setOutputFormat(LuceneOutputFormat.class);

        try {
            conf.set(LuceneOutputFormat.SINK_FIELDS_KEY, HadoopUtil.serializeBase64(getSinkFields()));
            conf.set(LuceneOutputFormat.INDEX_SETTINGS_KEY, HadoopUtil.serializeBase64(_indexSettings));
            conf.set(LuceneOutputFormat.STORE_SETTINGS_KEY, HadoopUtil.serializeBase64(_storeSettings));
        } catch (IOException e) {
            throw new TapException("Can't serialize sink fields", e);
        }

        conf.setInt(LuceneOutputFormat.MAX_SEGMENTS_KEY, _maxSegments);
        conf.setBoolean(LuceneOutputFormat.HAS_BOOST_KEY, _hasBoost);
        conf.setClass(LuceneOutputFormat.ANALYZER_KEY, _analyzer, Analyzer.class);
        conf.setInt(LuceneOutputFormat.MAX_FIELD_LENGTH_KEY, _maxFieldLength);
        
        LOGGER.info("Initializing Lucene index tap");
        Fields fields = getSinkFields();
        for (int i = 0; i < fields.size() - 1; i++) {
            LOGGER.info("  Field " + fields.get(i) + ": " + _storeSettings[i] + ", " + _indexSettings[i]);
        }
    }

    @Override
    public boolean source(FlowProcess<JobConf> conf, SourceCall<Object[], RecordReader<Tuple, Tuple>> sourceCall) throws IOException {
        throw new TapException("LuceneScheme can only be used as a sink, not a source");
    }

    @Override
    public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Void, OutputCollector<Tuple, Tuple>> sinkCall) throws IOException {
        sinkCall.getOutput().collect(Tuple.NULL, sinkCall.getOutgoingEntry().getTuple());
    }


}
