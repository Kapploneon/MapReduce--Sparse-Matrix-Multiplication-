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

import org.apache.hadoop.io.WritableComparable;
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

public class PairKey implements WritableComparable<PairKey>{
    private int row;
    private int col;

    public PairKey(){}
    public PairKey(int a, int b){
        set(a,b);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(row);
        dataOutput.writeInt(col);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        row = dataInput.readInt();
        col = dataInput.readInt();
    }

    public int getRow(){
        return row;
    }

    public int getCol(){
        return col;
    }

    public void set(int row, int col){
       this.row = row;
       this.col = col;
    }

    @Override
    public int compareTo(PairKey o) {

        if( this.row==o.row)
        {
            return this.col - o.col;
        }
        else return this.row - o.row;

    }
}
