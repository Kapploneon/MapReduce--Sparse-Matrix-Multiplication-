package BigData;


import java.io.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PairWritable implements Writable{


   private int order;
   private int val;
   private String MatrixName;

    public PairWritable(){}

   public PairWritable(int order, int val, String MatrixName){set(order,val,MatrixName);}

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeInt(order);
       dataOutput.writeInt(val);
       dataOutput.writeBytes(MatrixName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       order = dataInput.readInt();
       val = dataInput.readInt();
       MatrixName = dataInput.readLine();
    }

    public int getOrder(){
       return order;
    }

    public int getVal(){
        return val;
    }

    public String getMatrixName(){return MatrixName;}

    public void set(int a, int b, String name){
       this.order = a;
       this.val = b;
       this.MatrixName = name;
    }

}
