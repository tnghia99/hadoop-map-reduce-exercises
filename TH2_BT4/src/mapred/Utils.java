package src.mapred;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;

public class Utils {
    public static class Cau4aMapper extends Mapper<Object, Text, Text, IntWritable> {
        IntWritable one = new IntWritable(1);

        Text date = new Text();

        public void map(Object param1Object, Text value, Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            int inputPrice = Integer.parseInt(configuration.get("price")); //giá trị price được lấy từ args nhập vào khi chạy chương trình
            //Chuyển dòng csv thành chuỗi
            String sLine = value.toString();
            //Cắt chuỗi dữ liệu thành các giá trị tương ứng từng cột, vì ở cột tên sản phẩm thì dữ liệu có dấu "," nên dùng regex 
            String[] valueAtColumn = sLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (!valueAtColumn[0].equals("ten")) { //tránh dòng dữ liệu chứa tên cột, tên cột đầu tiên là "ten"
                //Cột thứ 2 là giá check dữ liệu thì chỉ là số nguyên, nên cắt bỏ .00000 phía sau lấy phần nguyên phía trước
                int iPrice = Integer.parseInt(valueAtColumn[1].split("\\.")[0]);  
                String strDate = valueAtColumn[3].split(" ")[0]; // Chuỗi ngày chỉ cần lấy phần ngày tháng năm
                if (iPrice > inputPrice) {
                    this.date.set(strDate);
                    context.write(this.date, this.one); //chỉ đếm số sản phẩm theo ngày =>output phase map có key là ngày, value: đếm số lượng
                }
            }
        }
    }

    public static class Cau4aReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
                throws IOException, InterruptedException {
            int i = 0; 
            //Đưa các kết quả từ phase map đã được gom theo khóa là ngày, chỉ cần cộng các giá trị value
            for (IntWritable intWritable : value)
                i += intWritable.get();
            this.result.set(i);
            context.write(key, this.result); 
        }
    }

    public static class Cau4bMapper extends Mapper<Object, Text, Text, IntWritable> {
        IntWritable price = new IntWritable();

        Text productName = new Text();

        public void map(Object param1Object, Text value, Context Context) throws IOException, InterruptedException {
            String sLine = value.toString();
            String[] valueAtColumn = sLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (!valueAtColumn[0].equals("ten")) {
                int i = Integer.parseInt(valueAtColumn[1].split("\\.")[0]);
                this.price.set(i);
                this.productName.set(valueAtColumn[0]);
                //cần tính giá trị trung bình nên phase map cho output có key là tên sản phẩm, value là giá
                Context.write(this.productName, this.price);
            }
        }
    }
                                                 
    public static class Cau4bReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context Context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable price : values) {
                sum += price.get();
                count++;
            }
            //vì giá trị trung bình là số thực nên output đầu ra của reducer là floatWriable
            FloatWritable avgPrice = new FloatWritable(sum / count);
            Context.write(key, avgPrice);
        }
    }

    public static class Cau4cMapper extends Mapper<Object, Text, Text, IntWritable> {
        IntWritable price = new IntWritable();

        Text productName = new Text();

        public void map(Object param1Object, Text value, Context Context) throws IOException, InterruptedException {
            String sLine = value.toString();
            String[] valueAtColumn = sLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (!valueAtColumn[0].equals("ten")) {
                int iPrice = Integer.parseInt(valueAtColumn[1].split("\\.")[0]);                                                               
                this.price.set(iPrice);
                this.productName.set(valueAtColumn[0]);
                Context.write(productName, price);
            }
        }
    }

    public static class Cau4cReducer extends Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> value, Context Context)
                throws IOException, InterruptedException {
            int highestPrice = 0;
            int lowestPrice = value.iterator().next().get();
            for (IntWritable price : value) {
                if (price.get() > highestPrice)
                    highestPrice = price.get();
                if (price.get() < lowestPrice)
                    lowestPrice = price.get();
            } 
            //value đầu ra là chuỗi cho biết thông giá cao nhất thấp nhất ứng với sản phẩm
            String result = "Highest Price: " + highestPrice + " & Lowest Price : " + lowestPrice;
            Context.write(key, new Text(result));
        }
    }

}