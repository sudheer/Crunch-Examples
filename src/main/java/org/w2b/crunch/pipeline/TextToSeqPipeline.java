package org.w2b.crunch.pipeline;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.To;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TextToSeqPipeline extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new TextToSeqPipeline(), args);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: hadoop jar crunch-demo-1.0-SNAPSHOT-job.jar"
                                      + " [generic options] input output");
            System.err.println();
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return 1;
        }

        String inputPath = args[0];
        String outputPath = args[1];
        
        // Create an object to coordinate pipeline creation and execution.
        Pipeline pipeline = new MRPipeline(TextToSeqPipeline.class, getConf());
        
        // Reference a given avro file as a collection of Record's.
        PCollection<String> records = pipeline.readTextFile(inputPath);
        
        // Instruct the pipeline to write the resulting records to a sequence file.
        pipeline.write(records,To.sequenceFile(outputPath));

        // Execute the pipeline as a MapReduce.
        PipelineResult result = pipeline.done();
        
        return result.succeeded() ? 0 : 1;
    }

}
