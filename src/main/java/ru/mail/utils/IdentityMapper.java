package ru.mail.utils;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class IdentityMapper extends Mapper<Object, Text, Text, Text> {
    String fileName;

    protected void setup(Mapper.Context context) throws java.io.IOException, java.lang.InterruptedException {
        fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    }

    @Override
    public void map(Object offset, Text value,
                    Context contex) throws IOException, InterruptedException {
        Text key = new Text(fileName + "-" + offset.toString());
        contex.write(key, value);
    }
}
