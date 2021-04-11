import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable<Color> {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */
    public Color () {}

    public Color ( short t, short i ) {
        type = t;
        intensity = i;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(type);
        out.writeShort(intensity);
    }

    public void readFields ( DataInput in ) throws IOException {
        type = in.readShort();
        intensity = in.readShort();
    }

    public String toString()
    {
        return  type + " " + intensity;
    }
       public int compareTo(Color obj) {
       int cmp = Short.compare(type, obj.type);
       if(cmp!=0)
       {
        return cmp;
       }
       return Short.compare(intensity, obj.intensity);
     }
}

public class Histogram {
    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */
	    String colors[] = value.toString().split(",");
            for (int j =0; j<colors.length; j++)
            {
                Color color =new Color((short)(j+1), Short.valueOf(colors[j]));
                context.write(color, new IntWritable (1));
            }
        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
                LongWritable answer = new LongWritable();
                long sum = 0;
                for (IntWritable v : values) {
                        sum += v.get();
                }
                answer.set(sum);
                context.write(key, answer);

	 }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */
        Job job = Job.getInstance ();
        job. setJarByClass (Histogram.class);
        job.setJobName("assignment1_Histogram");
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Histogram.HistogramMapper.class);
        job.setReducerClass(Histogram.HistogramReducer.class);
        job.waitForCompletion(true);
	 }
}


//code format
// import java.io.*;
// import java.util.Scanner;
// import java.util.Vector;

// import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.fs.FileSystem;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.*;
// import org.apache.hadoop.mapreduce.lib.input.*;
// import org.apache.hadoop.mapreduce.lib.output.*;


// /* single color intensity */
// class Color implements WritableComparable {
//     public short type;       /* red=1, green=2, blue=3 */
//     public short intensity;  /* between 0 and 255 */
//     /* need class constructors, toString, write, readFields, and compareTo methods */
// }


// public class Histogram {
//     public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
//         @Override
//         public void map ( Object key, Text value, Context context )
//                         throws IOException, InterruptedException {
//             /* write your mapper code */
//         }
//     }

//     public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
//         @Override
//         public void reduce ( Color key, Iterable<IntWritable> values, Context context )
//                            throws IOException, InterruptedException {
//             /* write your reducer code */
//         }
//     }

//     public static void main ( String[] args ) throws Exception {
//         /* write your main program code */
//    }
// }

