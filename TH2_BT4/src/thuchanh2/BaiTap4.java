package src.thuchanh2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;

import src.mapred.Utils.Cau4aMapper;
import src.mapred.Utils.Cau4aReducer;
import src.mapred.Utils.Cau4bMapper;
import src.mapred.Utils.Cau4bReducer;
import src.mapred.Utils.Cau4cMapper;
import src.mapred.Utils.Cau4cReducer;

public class BaiTap4 {
    /** 
     * @param args 
     * 4a: 4 args (a, inputfile, outputfile, price)
     * 4b: 3 args (b, inputfile, outputfile)
     * 4c: 3 args (c, inputfile, outputfile)
    */
    
    
    public static void main(String[] args) throws Exception {
        if (args.length == 4 && args[0].equalsIgnoreCase("a")) {
            Configuration conf = new Configuration(); // Khởi tạo config
            conf.set("price", args[3]);
            Job job = Job.getInstance(conf, "BaiTap4"); // Tạo job MapReduce với config và tên chương trình
            job.setJarByClass(BaiTap4.class); // Lớp chính
            job.setMapperClass(Cau4aMapper.class);
            job.setCombinerClass(Cau4aReducer.class); // Lớp Combiner (dùng chung với Reducer)
            job.setReducerClass(Cau4aReducer.class); // Lớp Reducer
            job.setOutputKeyClass(Text.class); // Chỉ định lớp của key trong output
            job.setOutputValueClass(IntWritable.class); // Chỉ định lớp của value trong output
            FileInputFormat.addInputPath(job, new Path(args[1])); // Đọc tham số, gán nó là dữ liệu input
            FileOutputFormat.setOutputPath(job, new Path(args[2])); // Đọc tham số, gán là đường dẫn kết quả output
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else if (args.length == 3 && args[0].equalsIgnoreCase("b")) {
            Configuration conf = new Configuration(); // Khởi tạo config
            Job job = Job.getInstance(conf, "BaiTap4"); // Tạo job MapReduce với config và tên chương trình
            job.setJarByClass(BaiTap4.class); // Lớp chính
            job.setMapperClass(Cau4bMapper.class);
            job.setReducerClass(Cau4bReducer.class); // Lớp Reducer
            job.setMapOutputKeyClass(Text.class); // Chỉ định lớp của key trong output
            job.setMapOutputValueClass(IntWritable.class); // Chỉ định lớp của value trong output
            job.setOutputKeyClass(Text.class); // Chỉ định lớp của key trong output
            job.setOutputValueClass(FloatWritable.class); // Chỉ định lớp của value trong output
            FileInputFormat.addInputPath(job, new Path(args[1])); // Đọc tham số, gán nó là dữ liệu input
            FileOutputFormat.setOutputPath(job, new Path(args[2])); // Đọc tham số, gán là đường dẫn kết quả output
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else if (args.length == 3 && args[0].equalsIgnoreCase("c")) {
            Configuration conf = new Configuration(); // Khởi tạo config
            Job job = Job.getInstance(conf, "BaiTap4"); // Tạo job MapReduce với config và tên chương trình
            job.setJarByClass(BaiTap4.class); // Lớp chính
            job.setMapperClass(Cau4cMapper.class);
            job.setReducerClass(Cau4cReducer.class); // Lớp Reducer
            job.setMapOutputKeyClass(Text.class); // Chỉ định lớp của key trong output
            job.setMapOutputValueClass(IntWritable.class); // Chỉ định lớp của value trong output
            job.setOutputKeyClass(Text.class); // Chỉ định lớp của key trong output
            job.setOutputValueClass(Text.class); // Chỉ định lớp của value trong output
            FileInputFormat.addInputPath(job, new Path(args[1])); // Đọc tham số, gán nó là dữ liệu input
            FileOutputFormat.setOutputPath(job, new Path(args[2])); // Đọc tham số, gán là đường dẫn kết quả output
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else { // Nếu người dùng nhập không đủ tham số thì thông báo cho người dùng biết cú pháp
            System.out.println("[ERROR] Sai cú pháp chương trình!");
            System.out.println("Cú pháp: BaiTap4 <a|b|c|d> <input> <output> <price neu la cau a> ");
        }
    }
}