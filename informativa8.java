/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.fundamentosdebigdata;


/**
 *
 * @author FbioHenrique3
 */


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Informacao8 {

    public static class MapperInformacao8 extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                
                String[] fields = value.toString().split(";");
                if (fields.length == 10) {
                   
                    Text keyField = new Text(fields[1] + "-" + fields[3]);
                    LongWritable valueMap = new LongWritable(Long.parseLong(fields[6]));

                    context.write(keyField, valueMap);

                }
            } catch (IOException | InterruptedException | NumberFormatException err) {
                System.out.println(err);
            }

        }

    }

    public static class ReducerInformacao8 extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long i = 0;

            for (LongWritable intvalues : values) {
                i += intvalues.get();
            }

            context.write(key, new LongWritable(i));

        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_inteira.csv";
        String arquivoSaida = "/home2/ead2021/SEM1/FbioHenrique3/Desktop/atpfundamentosdebigdata/Informacao8";

        if (args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, Informacao8.class.getSimpleName());

        job.setJarByClass(Informacao8.class);
        job.setMapperClass(MapperInformacao8.class);
        job.setReducerClass(ReducerInformacao8.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }
    
}