package BigData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output   .TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class MatrixMul extends Configured implements org.apache.hadoop.util.Tool {
    @Override
    public int run(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf,"MatrixMul");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job.setJarByClass(MatrixMul.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.setInputPaths(job,in);
        FileOutputFormat.setOutputPath(job,out);

        job.setNumReduceTasks(1);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
      //  -------------------------------------------------

        Configuration conf2 = new Configuration();

        Job job2 = null;
        try {
            job2 = Job.getInstance(conf2,"MatrixMul2");
        } catch (IOException e) {
            e.printStackTrace();
        }
        job2.setJarByClass(MatrixMul.class);

        Path secondInput = new Path(args[2]);
     //   Path finalOutput = new Path(args[3]);

        MultipleInputs.addInputPath(job2, out, KeyValueTextInputFormat.class, MatrixMul.Map2.class);
      //  FileInputFormat.setInputPaths(job2,secondInput);
        FileOutputFormat.setOutputPath(job2,secondInput);

        job2.setMapperClass(MatrixMul.Map2.class);
        job2.setReducerClass(MatrixMul.Reduce2.class);

        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapOutputKeyClass(BigData.PairKey.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        System.exit(job2.waitForCompletion(true)?0:1);

        return 0;
    }

    public static class Map2 extends Mapper<Text, Text, PairKey, IntWritable>{

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
            String[] val=value.toString().split(",");
         //   String[] num = line.split
         //   System.out.println("First: "+line[0]+"Second: "+line[1]+"Count: "+value.toString());
        //    System.out.println("First: "+line[0]);
        //    System.out.println("Count: "+value.toString());
            int i = Integer.parseInt(val[0]);
            int j = Integer.parseInt(val[1]);

            int product = Integer.parseInt(key.toString());

            PairKey pair = new PairKey(i,j);

            IntWritable prod = new IntWritable(product);
            System.out.println("Mapper 2 Index:"+i+","+j+"product :"+product);

                context.write(pair,prod);
            }
        }

    public static class Reduce2 extends Reducer<PairKey, IntWritable, Text, Text>{

        public void reduce(PairKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for (IntWritable val:values
                 ) {
              sum +=val.get();
            }
            String result = String.valueOf(sum);
            String position = key.getRow()+","+key.getCol();
            System.out.println("Final Index :"+position+" "+"Value :"+sum);
            context.write(new Text(position),new Text(result));

        }
    }

            public static class Map extends Mapper<LongWritable, Text, IntWritable, PairWritable>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] line=value.toString().split("[,\\s]+");

            String MatrixName = String.valueOf(line[0]);
            IntWritable Mapkey = new IntWritable();
            if(MatrixName.equals("A")){
              Mapkey.set(Integer.parseInt(line[2]));
              PairWritable pairVal = new PairWritable(Integer.parseInt(line[1]),Integer.parseInt(line[3]),MatrixName);
             // System.out.println("Index as col for A  : "+Mapkey.get()+" "+"Key as row,value,A :"+line[1]+" "+line[3]+" "+MatrixName);
                System.out.println("Index as col for A  : "+Mapkey.get()+" "+"Key as row,value,A :"+pairVal.getOrder()+" "+pairVal.getVal()+" "+MatrixName);
                context.write(Mapkey,pairVal);
            }
            else{
                Mapkey.set(Integer.parseInt(line[1]));
                PairWritable pairVal = new PairWritable(Integer.parseInt(line[2]),Integer.parseInt(line[3]),MatrixName);
           //     System.out.println("Index as row for B  : "+Mapkey.get()+" "+"Key as col,value,B :"+line[2]+" "+line[3]+" "+MatrixName);
                System.out.println("Index as row for B  : "+Mapkey.get()+" "+"Key as col,value,B :"+pairVal.getOrder()+" "+pairVal.getVal()+" "+MatrixName);
                context.write(Mapkey,pairVal);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, PairWritable, Text, Text>{
              int count=0;
           public void reduce(IntWritable key, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException{
               LinkedList<PairWritable> arrlistA = new LinkedList<>();
               LinkedList<PairWritable> arrlistB = new LinkedList<>();
               System.out.println("Reducer Count :"+count);
               count++;
            for (PairWritable val:values
                 ) {

                String checkA = "A";
                if(val.getMatrixName().equals(checkA))
                {
                    System.out.println("Val A"+val.getOrder()+","+key.get()+":"+val.getVal());

                    if(arrlistB.size()==0){
                        System.out.println("B is empty");
                        PairWritable temp = new PairWritable(val.getOrder(),val.getVal(),val.getMatrixName());
                        arrlistA.add(temp);
                    }
                    else {

                        for (PairWritable b : arrlistB
                                ) {

                            int A = b.getVal();
                            int B = val.getVal();

                            System.out.println("b.getval() "+A);
                            System.out.println("val.getval() "+B);

                            int result = A * B;
                            Text Reducekey = new Text(String.valueOf(result));
                            String temp = val.getOrder() + "," + b.getOrder();
                            System.out.println("Reducer Index new A :" + temp + " " + result);

                            context.write(Reducekey, new Text(temp));
                        }
                        PairWritable temp1 = new PairWritable(val.getOrder(),val.getVal(),val.getMatrixName());
                        arrlistA.add(temp1);
                    }
                }
                else{
                    System.out.println("Val B"+key.get()+","+val.getOrder()+":"+val.getVal());
                    if(arrlistA.size()==0){
                        System.out.println("A is empty");
                        PairWritable temp = new PairWritable(val.getOrder(),val.getVal(),val.getMatrixName());
                        arrlistB.add(temp);
                    }

                    else {

                        for (PairWritable a : arrlistA
                                ) {
                            int A = a.getVal();
                            int B = val.getVal();

                            System.out.println("a.getval() "+A);
                            System.out.println("val.getval() "+B);
                            int result = A * B;
                            Text Reducekey = new Text(String.valueOf(result));
                            String temp = a.getOrder() + "," + val.getOrder();
                            System.out.println("Reducer Index new B :" + temp + " " + result);
                            context.write(Reducekey, new Text(temp));

                        }
                        PairWritable temp1 = new PairWritable(val.getOrder(),val.getVal(),val.getMatrixName());
                        arrlistB.add(temp1);
                    }
                }
            }


        }

    }
    //  String res = finalResult.toString();
    //   Text tres = new Text(res);







    public static void main (String[] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new MatrixMul(),args);
        System.exit(res);
    }
}
