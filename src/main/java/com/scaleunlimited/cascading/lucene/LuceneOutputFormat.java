package com.scaleunlimited.cascading.lucene;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LimitTokenCountAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Version;

import cascading.flow.hadoop.util.HadoopUtil;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("deprecation")
public class LuceneOutputFormat extends FileOutputFormat<Tuple, Tuple> {
// public class LuceneOutputFormat implements OutputFormat<Tuple, Tuple> {
    private static final Logger LOGGER = Logger.getLogger(LuceneOutputFormat.class);
    
    public static final String SINK_FIELDS_KEY = "com.scaleunlimited.cascading.lucene.sinkFields";
    public static final String MAX_SEGMENTS_KEY = "com.scaleunlimited.cascading.lucene.maxSegments";

    public static final String ANALYZER_KEY = "bixo.indexer.analyzer";
    public static final String MAX_FIELD_LENGTH_KEY = "bixo.indexer.maxFieldLength";
    public static final String HAS_BOOST_KEY = "bixo.indexer.hasBoost";
    public static final String INDEX_SETTINGS_KEY = "bixo.indexer.indexSettings";
    public static final String STORE_SETTINGS_KEY = "bixo.indexer.storeSettings";
    
    public static final int DEFAULT_MAX_SEGMENTS = 10;

    private static class LuceneRecordWriter implements RecordWriter<Tuple, Tuple> {

        private Progressable _progress;
        private Path _outputPath;
        private FileSystem _outputFS;
        private Fields _sinkFields;
        private int _maxSegments;
        private Index[] _index;
        private Store[] _store;
        private boolean _hasBoost;
        private IndexWriter _indexWriter;
        private File _localIndexFolder;
        private String _jobName;
        
        public LuceneRecordWriter(JobConf conf, String name, Progressable progress) {
            _jobName = name;
            _progress = progress;

            try {
                // Figure out where ultimately the results need to wind up.
                _outputPath = FileOutputFormat.getTaskOutputPath(conf, name);
                _outputFS = _outputPath.getFileSystem(conf);

                // Get the set of fields we're indexing.
                _sinkFields = (Fields)HadoopUtil.deserializeBase64(conf.get(SINK_FIELDS_KEY));

                _maxSegments = conf.getInt(MAX_SEGMENTS_KEY, DEFAULT_MAX_SEGMENTS);

                int maxFieldLength = conf.getInt(MAX_FIELD_LENGTH_KEY, Integer.MAX_VALUE);
                Analyzer analyzer = new LimitTokenCountAnalyzer((Analyzer)ReflectionUtils.newInstance(conf.getClass(ANALYZER_KEY, Analyzer.class), conf), maxFieldLength);
                _index = (Index[])HadoopUtil.deserializeBase64(conf.get(INDEX_SETTINGS_KEY));
                _store = (Store[])HadoopUtil.deserializeBase64(conf.get(STORE_SETTINGS_KEY));

                _sinkFields = (Fields)HadoopUtil.deserializeBase64(conf.get(SINK_FIELDS_KEY));
                _hasBoost = conf.getBoolean(HAS_BOOST_KEY, false);

                String tmpFolder = System.getProperty("java.io.tmpdir");
                _localIndexFolder = new File(tmpFolder, UUID.randomUUID().toString());
                final Directory localIndexDirectory = new MMapDirectory(_localIndexFolder);

                final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);
                // TODO support setting up max buffered docs/other settings here.
                _indexWriter = new IndexWriter(localIndexDirectory, iwc);
            } catch (Exception e) {
                throw new TapException("Can't create LuceneRecordWriter", e);
            }
        }
        
        @Override
        public void write(Tuple key, Tuple value) throws IOException {
            Document doc = new Document();
            
            int size = _sinkFields.size();
            if (_hasBoost) {
                size -= 1;
            }
            
            for (int i = 0; i < size; i++) {
                Object fieldValue = value.getObject(i);
                if (fieldValue == null) {
                    // Don't add null values.
                } else if (fieldValue instanceof Tuple) {
                    Tuple list = (Tuple)fieldValue;
                    for (int j = 0; j < list.size(); j++) {
                        safeAdd(doc, i, list.getObject(j).toString());
                    }
                } else {
                    safeAdd(doc, i, fieldValue.toString());
                }
            }
            
            // We append the boost field at the end, so it's always the last value in the tuple.
            if (_hasBoost) {
                float boost = value.getFloat(size);
                doc.setBoost(boost);
            }
            
            _indexWriter.addDocument(doc);
        }

        private void safeAdd(Document doc, int fieldIndex, String value) {
            String fieldName = (String)_sinkFields.get(fieldIndex);

            if ((value != null) && (value.length() > 0)) {
                doc.add(new Field(fieldName, value.toString(), _store[fieldIndex], _index[fieldIndex]));
            }
        }
        

        @Override
        public void close(final Reporter reporter) throws IOException {
            // Hadoop need to know we still working on it.
            Thread reporterThread = startProgressThread();
            
            try {
                LOGGER.info("Optimizing index for " + _jobName);
                _indexWriter.commit();
                _indexWriter.forceMerge(_maxSegments);
                _indexWriter.close();

                copyToHDFS();
            } finally {
                reporterThread.interrupt();
            }
        }

        private void copyToHDFS() throws IOException {

            // HACK!!! Hadoop has a bug where a .crc file locally with the matching name will
            // trigger an error, so we want to get rid of all such .crc files from inside of
            // the index dir.
            removeCrcFiles(_localIndexFolder);

            LOGGER.info(String.format("Copying index from %s to %s", _localIndexFolder, _outputPath));
            _outputFS.copyFromLocalFile(true, new Path(_localIndexFolder.getAbsolutePath()), _outputPath);
        }
        
        private void removeCrcFiles(File dir) {
            File[] crcFiles = dir.listFiles(new FilenameFilter() {

                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".crc");
                }
            });
            
            for (File crcFile : crcFiles) {
                crcFile.delete();
            }
        }
        
        /**
         * Fire off a thread that repeatedly calls Hadoop to tell it we're making progress.
         * @return
         */
        private Thread startProgressThread() {
            Thread result = new Thread() {
                @Override
                public void run() {
                    while (!isInterrupted()) {
                        _progress.progress();
                        
                        try {
                            sleep(10 * 1000);
                        } catch (InterruptedException e) {
                            interrupt();
                        }
                    }
                }
            };
            
            result.start();
            return result;
        }


    }
    
    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        // TODO anything to do here?
    }

    @Override
    public RecordWriter<Tuple, Tuple> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        return new LuceneRecordWriter(job, name, progress);
    }

}
