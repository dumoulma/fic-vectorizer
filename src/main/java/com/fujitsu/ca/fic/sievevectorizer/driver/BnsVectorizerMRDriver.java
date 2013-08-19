package com.fujitsu.ca.fic.sievevectorizer.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.sievevectorizer.job.BnsPigOutputToVectorMapper;
import com.fujitsu.ca.fic.sievevectorizer.job.VectorToSequenceReducer;

public class BnsVectorizerMRDriver extends Configured implements Tool {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusVectorizationDriver.class);

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BnsVectorizerMRDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = getConf();

        // String trainDir = "data/out/sieve/bns/spam-vs-rel/train";
        // String testDir = "data/out/sieve/bns/spam-vs-rel/test";
        // String outputFilename = "data/out/sieve/bns/spam-vs-rel/";
        String trainDir = conf.get("data.corpus.train.path");
        String testDir = conf.get("data.corpus.test.path");
        String outputFilename = conf.get("data.sequence.output.path");

        if (trainDir == null | testDir == null | outputFilename == null) {
            LOG.error("The configuration file was not loaded correctly! Please check: \n" + "data.corpus.train.path \n"
                    + "data.corpus.test.path \n" + "data.sequence.output.path \n");
            throw new IOException("The expected configuration values for data paths have not been found.");
        }

        Path trainInput = new Path(trainDir);
        Path testInput = new Path(testDir);
        Path trainOutput = new Path(outputFilename + "/train-seq");
        Path testOutput = new Path(outputFilename + "/test-seq");

        boolean testJobExitCode = vectorizeCorpus(conf, "BNS Vectorize train", trainInput, trainOutput);
        boolean trainJobExitCode = vectorizeCorpus(conf, "BNS Vectorize test", testInput, testOutput);

        int exitCode = testJobExitCode && trainJobExitCode ? 1 : 0;
        if (exitCode == 1) {
            LOG.info("Jobs Completed successfully!");
        }

        return exitCode;
    }

    private boolean vectorizeCorpus(Configuration conf, String jobName, Path input, Path output) throws IOException, InterruptedException,
            ClassNotFoundException {
        HadoopUtil.delete(conf, output);

        Job job = new Job(conf, jobName);
        job.setJarByClass(getClass());

        job.setMapperClass(BnsPigOutputToVectorMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(VectorWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, input);

        job.setReducerClass(VectorToSequenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true);
    }
}
