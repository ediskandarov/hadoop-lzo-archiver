package ru.mail.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ValuesReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
        Iterator<Text> it=values.iterator();
        while (it.hasNext()) {
            // context.write(key, it.next());
            context.write(it.next(), null);
        }
    }
}
