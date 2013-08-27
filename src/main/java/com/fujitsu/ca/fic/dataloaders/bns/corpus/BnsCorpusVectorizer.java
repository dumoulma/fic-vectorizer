package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.CorpusLoaderFactory;
import com.fujitsu.ca.fic.dataloaders.CorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.LineParser;

public class BnsCorpusVectorizer implements CorpusVectorizer {
	private static Logger log = LoggerFactory
			.getLogger(BnsCorpusVectorizer.class);

	private CorpusLoaderFactory<Vector> loaderFactory;

	public BnsCorpusVectorizer(CorpusLoaderFactory<Vector> factory) {
		this.loaderFactory = factory;
	}

	@Override
	public void convertToSequenceFile(Configuration conf, String inputDirName,
			String outputDirName, LineParser<Vector> lineParser) throws IOException {
		SequenceFile.Writer writer = null;
		try {
			Path outputPath = new Path(outputDirName);
			writer = SequenceFile.createWriter(FileSystem.get(conf), conf,
					outputPath, Text.class, VectorWritable.class);

			Iterable<Vector> corpusLoader = loaderFactory
					.buildLoader(conf, inputDirName, lineParser);
			long index = 0L;
			for (Vector nextVectorizedDocument : corpusLoader) {
				log.debug("Read " + index++ + "th vectorized document");
				String docLabel = ((NamedVector) nextVectorizedDocument)
						.getName();
				String[] parts = docLabel.split(",");
				String docName = parts[0];
				String label = parts[1];

				writer.append(new Text(docName), new VectorWritable(
						new NamedVector(nextVectorizedDocument, label)));
			}
			log.info("Sequence file written to HDFS successfully. Docs written: "
					+ index);
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
	}
}
