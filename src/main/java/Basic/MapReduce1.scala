package Basic

import org.apache.hadoop
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{MapWritable, NullWritable}

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.mapreduce.{Job, Mapper}
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.util.Tool

/**
  * Created by jingwang on 4/1/16.
  */
object MapReduce1 extends Configured with Tool {


   def run(args: Array[String]): Int = {
    val conf: Configuration = new Configuration()
    val inputPath = args(0)
    val outputPath = args(1)
    val job: Job = Job.getInstance(conf)

    FileInputFormat.addInputPath(job, new Path(inputPath))

    job.setJobName(s"Mapreduce1:inputpath ${inputPath}, outputPath:${outputPath}")
    job.setInputFormatClass(classOf[FileInputFormat[NullWritable, MapWritable]])
    job.setOutputFormatClass(classOf[FileOutputFormat[NullWritable, MapWritable]])
    job.setOutputKeyClass(classOf[NullWritable])
    job.setOutputValueClass(classOf[MapWritable])

    job.setMapperClass(classOf[MapReduce1Mapper])
    job.setMapOutputKeyClass(classOf[NullWritable])
    job.setMapOutputValueClass(classOf[MapWritable])
    job.setJarByClass(classOf[MapReduce1Mapper])
    FileOutputFormat.setOutputPath(job, new Path(outputPath))
     if(job.waitForCompletion(true)) 0
     else 1

  }
  def main(args: Array[String]): Unit = {ToolRunner.run(new Configuration(),this,args)
  }
}

class MapReduce1Mapper extends Mapper[NullWritable, MapWritable, NullWritable, MapWritable] {

  override def map(key: NullWritable, value: MapWritable, context: Mapper[NullWritable, MapWritable, NullWritable, MapWritable]#Context): Unit = {

    context.write(key, value)

  }


}