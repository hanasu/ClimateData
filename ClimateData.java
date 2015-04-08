/*
 * Copyright (C) 2015 Brendan O'Dowd
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;

/**
 *
 * @author Brendan O'Dowd
 */
public class ClimateData {
    
    public static class Map extends MapReduceBase implements 
            Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final Text id = new Text();
        private final IntWritable temp = new IntWritable();
        
        @Override
        public void map(LongWritable key, Text value, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) 
                throws IOException {

            String line = value.toString();
            id.set(line.substring(0,10)); 
        
            if (line.charAt(19) == 'A') {
                
                for(int i = 21; i < 266; i += 8) {
                    
                    temp.set(Integer.parseInt(line.substring(i,i+4)));
                    
                    if ( temp.get() != -9999 ) {
                        
                       System.out.println("ID: " + id + " Temp: " + temp);
                       output.collect(id, temp); 
                    
                    }  
                }
            }
        }
    }
    
    public static class Reduce extends MapReduceBase implements 
            Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterator<IntWritable> values, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) 
                throws IOException {
            int count = 0;
            int total = 0;
            
            while(values.hasNext()) {
                total += values.next().get();
                count++;
            }
            
            output.collect(key, new IntWritable(total/count));
        }
    }
    
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(ClimateData.class);
        conf.setJobName("climatedata");
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        JobClient.runJob(conf);
    }
}