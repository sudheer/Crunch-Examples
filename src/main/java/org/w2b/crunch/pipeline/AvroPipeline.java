package org.w2b.crunch.pipeline;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w2b.crunch.dofn.OrderTotalDoFn;
import org.w2b.crunch.filterfn.OrdersFilterFn;
import org.w2b.crunch.mapfn.OrderItemsMapFn;
import org.w2b.crunch.mapfn.OrdersMapFn;

public class AvroPipeline extends Configured implements Tool {
  
    public static void main(String[] args) throws Exception {
      validateArgs(args);
      ToolRunner.run(new Configuration(), new AvroPipeline(), args);
  }

  public int run(String[] args) throws Exception {
    
    String order_items_path = args[0];
    String orders_path = args[1];
    String outputPath = args[2];
    
    // read the schema file
    ClassLoader classLoader = getClass().getClassLoader();
    File file1 = new File(classLoader.getResource("schema/order_items.avsc").getFile());
    File file2 = new File(classLoader.getResource("schema/orders.avsc").getFile());
    
    // prepare schema object from avsc.
    Schema order_items_schema = new Schema.Parser().parse(file1);
    Schema orders_schema = new Schema.Parser().parse(file2);
    
    // pipeline object to co-ordinate execution.
    Pipeline pipeline = new MRPipeline(AvroPipeline.class, getConf());
    
    PCollection<Record> order_items = pipeline.read(From.avroFile(order_items_path));
    
    PCollection<Record> orders = pipeline.read(From.avroFile(orders_path));
    
    PCollection<Record> filtered_order_items = order_items.filter("filters records more than 1 order", new OrdersFilterFn());
    
    PTable<String,Record> order_items_map = filtered_order_items.parallelDo("", new OrderItemsMapFn(), Avros.tableOf(Avros.strings(), Avros.generics(order_items_schema)));

    PTable<String,Record> orders_map = orders.parallelDo("", new OrdersMapFn(), Avros.tableOf(Avros.strings(), Avros.generics(orders_schema)));

    PTable<String,Pair<Record,Record>> orders_join = order_items_map.join(orders_map);
    
    PGroupedTable<String, Pair<Record,Record>> order_group = orders_join.groupByKey();
    
    PTable<String,Double> total = order_group.parallelDo(new OrderTotalDoFn(), Avros.tableOf(Avros.strings(), Avros.doubles()) );
    
    total.write(To.textFile(new Path(outputPath)));
    
    getConf().set("crunch.planner.dotfile.outputdir", "/tmp/crunch-demo/dot/");
    
    return pipeline.done().succeeded() ? 0 : 1;
  }
  
  public static void validateArgs(String[] args) throws Exception {
      if(args.length != 3) {
        throw new Exception("hadoop jar Crunch-Examples-0.0.1-SNAPSHOT.jar org.w2b.crunch.pipeline.WordCountPipeline <input1> <input2> <output>");
      }
  }

}
