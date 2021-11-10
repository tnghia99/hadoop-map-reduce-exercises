package src.thuchanh2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import src.mapred.Couple;
import src.mapred.Triad;
import src.mapred.ProbAB;
import src.mapred.ProbABC;
import src.mapred.Utils.*;

/**
 * Lớp chứa hàm chính của chương trình
 */
public class BaiTap3 {
    public static void main(String[] args) throws Exception {

        if (args.length == 3) { // Kiểm tra xem người dùng đã nhập đủ các tham số chưa
            // Thiết lập toàn bộ các cài đặt cho chương trình MapReduce
            Configuration conf = new Configuration(); // Khởi tạo config
            Job job = Job.getInstance(conf, "BaiTap3"); // Tạo job MapReduce với config và tên chương trình
            job.setJarByClass(BaiTap3.class); // Lớp chính
            if (args[0].equalsIgnoreCase("a")) { // Người dùng chọn giải bài a
                job.setMapperClass(CoupleMapper.class); // Chỉ định lớp Mapper là lớp bình thường
                System.out.println("[INFO] Sử dụng lớp Mapper bình thường"); // In thông tin ra cho người dùng
                job.setCombinerClass(CoupleReducer.class); // Lớp Combiner (dùng chung với Reducer)
                job.setReducerClass(CoupleReducer.class); // Lớp Reducer
                job.setOutputKeyClass(Couple.class); // Chỉ định lớp của key trong output
                job.setOutputValueClass(IntWritable.class); // Chỉ định lớp của value trong output
                FileInputFormat.addInputPath(job, new Path(args[1])); // Đọc tham số, gán nó là dữ liệu input
                FileOutputFormat.setOutputPath(job, new Path(args[2])); // Đọc tham số, gán là đường dẫn kết quả output
                System.exit(job.waitForCompletion(true) ? 0 : 1); // Đợi chương trình thực hiện xong và thoát
            }
            if (args[0].equalsIgnoreCase("b")) { // Người dùng chọn giải bài b

                // Lớp Mapper
                job.setMapperClass(CoupleStripesMapper.class);
                System.out.println("[INFO] Sử dụng lớp Mapper bình thường");

                // Trong quá trình xử lý này do không sử dụng phase Combiner vì thế không set class Combiner
                job.setReducerClass(CoupleStripesReducer.class); // Lớp Reducer
                job.setMapOutputKeyClass(IntWritable.class); // Chỉ định lớp của key khi output khỏi pha Map
                job.setMapOutputValueClass(Couple.class); // Chỉ định lớp của value khi output khỏi pha Map
                // Bắt buộc phải set lớp key và value của Mapper khi output của Mapper và Reducer khác nhau

                job.setOutputKeyClass(NullWritable.class); // Chỉ định lớp của key trong output
                job.setOutputValueClass(ProbAB.class); // Chỉ định lớp của value trong output
                FileInputFormat.addInputPath(job, new Path(args[1])); // Đọc tham số, gán nó là dữ liệu input
                FileOutputFormat.setOutputPath(job, new Path(args[2])); // Đọc tham số, gán là đường dẫn kết quả output
                System.exit(job.waitForCompletion(true) ? 0 : 1); // Đợi chương trình thực hiện xong và thoát
            }
            if(args[0].equalsIgnoreCase("c")) { //Người dùng chọn giải bài c
                job.setMapperClass(TriadMapper.class);
                job.setReducerClass(TriadReducer.class);
                job.setOutputKeyClass(Triad.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[1])); // Đọc tham số, gán nó là dữ liệu input
                FileOutputFormat.setOutputPath(job, new Path(args[2])); // Đọc tham số, gán là đường dẫn kết quả output
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            }
            if (args[0].equalsIgnoreCase("d")) { // Người dùng chọn giải bài d

                // Lớp Mapper
                job.setMapperClass(TriadStripesMapper.class);
                System.out.println("[INFO] Sử dụng lớp Mapper bình thường");

                // Trong quá trình xử lý này do không sử dụng phase Combiner vì thế không set class Combiner
                job.setReducerClass(TriadStripesReducer.class); // Lớp Reducer

                job.setMapOutputKeyClass(Couple.class); // Chỉ định lớp của key khi output khỏi pha Map
                job.setMapOutputValueClass(Couple.class); // Chỉ định lớp của value khi output khỏi pha Map
                // Bắt buộc phải set lớp key và value của Mapper khi output của Mapper và Reducer khác nhau

                job.setOutputKeyClass(NullWritable.class); // Chỉ định lớp của key trong output
                job.setOutputValueClass(ProbABC.class); // Chỉ định lớp của value trong output
                FileInputFormat.addInputPath(job, new Path(args[1])); // Đọc tham số, gán nó là dữ liệu input
                FileOutputFormat.setOutputPath(job, new Path(args[2])); // Đọc tham số, gán là đường dẫn kết quả output
                System.exit(job.waitForCompletion(true) ? 0 : 1); // Đợi chương trình thực hiện xong và thoát
            }
            
        } else { // Nếu người dùng nhập không đủ tham số thì thông báo cho người dùng biết cú pháp
            System.out.println("[ERROR] Sai cú pháp chương trình!");
            System.out.println("Cú pháp: BaiTap3 <a|b|c|d> <input> <output>");
        }
    }
}