package src.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lớp chứa các hàm cần thiết của chương trình như Mapper, Reducer
 */
public class Utils {

    /**
     * Lớp mở rộng từ Mapper với thiết lập <code>&lt;key, value&gt;</code> đầu vào
     * và <code>&lt;key, value&gt;</code> đầu ra của pha Map
     */
    public static class CoupleMapper extends Mapper<Object, Text, Couple, IntWritable> {

        // Tạo biến chứa giá trị 1
        // Tạo ở đây để không phải cấp phát bộ nhớ nhiều lần trong hàm map, gây lãng phí
        IntWritable one = new IntWritable(1);

        // Override lại hàm map
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Cắt văn bản đầu vào thành từng dòng, ứng với từng transaction, để xử lý
            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {

                // Trong một transaction, cắt lấy từng sản phẩm (item)
                String[] arrItems = sLine.split(" ");

                // Lấy số lượng các item đã cắt được
                int iLen = arrItems.length;

                // Khởi tạo mảng chứa các couple tạo ra
                // Dùng mảng để kiểm tra xem có tồn tại couple hay chưa
                ArrayList<Couple> arrCouple = new ArrayList<>();

                // Lần lượt duyệt qua các item
                // Tạo couple đồng hiện giữa nó và các item phía sau
                for (int i = 0; i < iLen - 1; i++) {
                    for (int j = i + 1; j < iLen; j++) {

                        // Không tạo couple nó với chính nó
                        if (!arrItems[i].equals(arrItems[j])) {
                            Couple cNew = new Couple(arrItems[i], arrItems[j]); // Tạo couple
                            if (!arrCouple.isEmpty()) {
                                // Biến cho biết đã tồn tại couple hay chưa
                                // Trong 1 tran thì chỉ tính đồng hiện 1 lần, AB giống BA
                                boolean bExist = false;
                                for (Couple c : arrCouple) {
                                    if (c.compareTo(cNew) == 0) {
                                        // Nếu có rồi thì ghi nhận lại và thoát, không thêm
                                        bExist = true;
                                        break;
                                    }
                                }
                                if (!bExist)
                                    arrCouple.add(cNew);// Nếu chưa có thì thêm couple vào mảng
                            } else {// Nếu mảng đang rỗng thì thêm couple vào (first couple)
                                arrCouple.add(cNew);
                            }
                        }
                    }
                }
                // Sau khi xử lý xong 1 tran (1 line) thì emit các couple
                // Mỗi couple đếm là 1 => <couple, 1>
                for (Couple c : arrCouple) {
                    context.write(c, one);
                }
            }
        }
    }

    /**
     * Lớp mở rộng từ Reducer với thiết lập <code>&lt;key, value&gt;</code> đầu vào
     * và <code>&lt;key, value&gt;</code> đầu ra của pha Reduce
     */
    public static class CoupleReducer extends Reducer<Couple, IntWritable, Couple, IntWritable> {

        // Tạo biến chứa kết quả của reduce
        // Tạo ở đây để không phải cấp phát/thu hồi bộ nhớ nhiều lần trong hàm reduce,
        // gây lãng phí tài nguyên
        IntWritable result = new IntWritable();

        @Override
        public void reduce(Couple key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Tạo biến chứa tổng các value
            int sum = 0;

            // Lần lượt duyệt qua mảng chứa các value có chung key đã được gộp lại
            // Cộng tất cả lại với nhau
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Chuyển đổi biến tổng thành kiểu dữ liệu phù hợp để ghi ra
            result.set(sum);
            context.write(key, result); // Ghi kết quả
        }
    }

    /**
     * Lớp mở rộng từ Mapper với thiết lập <code>&lt;key, value&gt;</code> đầu vào
     * và <code>&lt;key, value&gt;</code> đầu ra của pha Map. Có sử dụng kỹ thuật
     * Combiner trong pha Mapper và Duy trì trạng thái.
     */
    public static class CoupleMapperwCombiner extends Mapper<Object, Text, Couple, IntWritable> {

        // Khai báo một HashMap chứa các cặp <key, value> của toàn bộ các map, hiện thực
        // kỹ thuật Combiner trong Mapper và Duy trì trạng thái
        Map<Couple, Integer> hmCouple;

        // Định nghĩa lại hàm setup là hàm được gọi thực thi trước các hàm map
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            hmCouple = new HashMap<>(); // Khởi tạo AssociativeArray
            super.setup(context);
        }

        // Override lại hàm map
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Cắt văn bản đầu vào thành từng dòng, ứng với từng transaction, để xử lý
            String[] arrLines = value.toString().split("\n");

            // Nếu chỉ sử dụng Combiner trong Mapper không mà không dùng kỹ thuật Duy trì
            // trạng thái, thì mình sẽ khai báo một cái AssociativeArray tại đây, nó sẽ chứa
            // tất cả các cặp <key, value> ứng với tất cả các dòng dữ liệu trong 1 pha map

            for (String sLine : arrLines) {

                // Trong một transaction, cắt lấy từng sản phẩm (item)
                String[] arrItems = sLine.split(" ");

                // Lấy số lượng các item đã cắt được
                int iLen = arrItems.length;

                // Khởi tạo mảng chứa các couple tạo ra
                // Dùng mảng để kiểm tra xem có tồn tại couple hay chưa
                ArrayList<Couple> arrCoupleInOneLine = new ArrayList<>();

                // Lần lượt duyệt qua các item
                // Tạo couple đồng hiện giữa nó và các item phía sau
                for (int i = 0; i < iLen - 1; i++) {
                    for (int j = i + 1; j < iLen; j++) {

                        // Không tạo couple nó với chính nó
                        if (!arrItems[i].equals(arrItems[j])) {
                            Couple cNew = new Couple(arrItems[i], arrItems[j]); // Tạo couple
                            if (!arrCoupleInOneLine.isEmpty()) {
                                // Biến cho biết đã tồn tại couple hay chưa
                                // Trong 1 tran thì chỉ tính đồng hiện 1 lần, AB giống BA
                                boolean bExist = false;
                                for (Couple c : arrCoupleInOneLine) {
                                    if (c.compareTo(cNew) == 0) {
                                        // Nếu có rồi thì ghi nhận lại và thoát, không thêm
                                        bExist = true;
                                        break;
                                    }
                                }
                                if (!bExist)
                                    arrCoupleInOneLine.add(cNew);// Nếu chưa có thì thêm couple vào mảng
                            } else {// Nếu mảng đang rỗng thì thêm couple vào (first couple)
                                arrCoupleInOneLine.add(cNew);
                            }
                        }
                    }
                }
                // Sau khi xử lý xong 1 tran (1 line) thì thêm kết quả vào AssociativeArray
                for (Couple c : arrCoupleInOneLine) {
                    Integer i = hmCouple.get(c);
                    if (i != null)
                        hmCouple.replace(c, i.intValue() + 1);
                    else {
                        hmCouple.put(c, 1);

                        // Sử dụng kỹ thuật block and flush để chống tràn bộ nhớ

                        // if (hmCouple.size() >= 10000) { // ngưỡng là 10000 cặp giá trị
                        // System.out.println("[INFO] Block and Flush!!!");
                        // emit(context);
                        // hmCouple.clear();
                        // }

                    }
                }
            }

            // Đối với việc chỉ sử dụng Combiner trong Mapper thì sẽ emit kết quả ở đây, sau
            // khi xử lý hết các input của một map
        }

        // Override lại hàm cleanup là hàm được gọi sau cùng khi đã hoàn tất các hàm map
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Thêm lần lượt các giá trị của AssociativeArray vào ngữ cảnh (emit)
            emit(context);
            super.cleanup(context);
        }

        /**
         * Hàm ghi nhận lại kết quả các cặp <code>&lt;key, value&gt;</code>
         * 
         * @param context ngữ cảnh đầu ra của hàm Mapper
         * @throws IOException
         * @throws InterruptedException
         */
        private void emit(Context context) throws IOException, InterruptedException {
            // Duyệt qua tất cả các phần tử trong AssociativeArray và ghi nhận lại từng cái
            // vào trong context
            for (Entry<Couple, Integer> oneItem : hmCouple.entrySet()) {
                context.write(oneItem.getKey(), new IntWritable(oneItem.getValue()));
            }
        }
    }

    /**
     * Lớp mở rộng từ Mapper với thiết lập <code>&lt;key, value&gt;</code> đầu vào
     * và <code>&lt;key, value&gt;</code> đầu ra của pha Map. Có sử dụng kỹ thuật
     * Stripes trong pha Mapper để giải quyết các bài toán phức tạp.
     */
    public static class CoupleStripesMapper extends Mapper<Object, Text, IntWritable, Couple> {

        // Tạo biến chứa giá trị 1
        // Tạo ở đây để không phải cấp phát bộ nhớ nhiều lần trong hàm map, gây lãng phí
        IntWritable one = new IntWritable(1);

        // Override lại hàm map
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Cắt văn bản đầu vào thành từng dòng, ứng với từng transaction, để xử lý
            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {

                // Trong một transaction, cắt lấy từng sản phẩm (item)
                String[] arrItems = sLine.split(" ");

                // Lấy số lượng các item đã cắt được
                int iLen = arrItems.length;

                // Khởi tạo mảng chứa các couple tạo ra
                // Dùng mảng để kiểm tra xem có tồn tại couple hay chưa
                ArrayList<Couple> arrCouple = new ArrayList<>();

                // Đối với từng item tạo cặp với item ảo là -1, sau này sẽ dùng để đếm item đó
                // có trong bao nhiêu tran
                for (String sItem : arrItems) {
                    arrCouple.add(new Couple(sItem, "-1"));
                }

                // Lần lượt duyệt qua các item
                // Tạo couple đồng hiện giữa nó và các item phía sau
                for (int i = 0; i < iLen - 1; i++) {
                    for (int j = i + 1; j < iLen; j++) {

                        // Không tạo couple nó với chính nó
                        if (!arrItems[i].equals(arrItems[j])) {
                            Couple cNew = new Couple(arrItems[i], arrItems[j]); // Tạo couple
                            // Trong 1 tran thì chỉ tính đồng hiện 1 lần, AB giống BA
                            boolean bExist = false; // Biến cho biết đã tồn tại couple hay chưa
                            for (Couple c : arrCouple) {
                                if (c.equals(cNew)) {
                                    // Nếu có rồi thì ghi nhận lại và thoát, không thêm
                                    bExist = true;
                                    break;
                                }
                            }
                            if (!bExist)
                                arrCouple.add(cNew);// Nếu chưa có thì thêm couple vào mảng
                        }
                    }
                }
                // Sau khi xử lý xong 1 tran (1 line) thì emit các couple
                // Mỗi couple trả về 2 cặp <key, value> là <item1, (item2, 1)> và
                // <item2, (item1, 1)>
                // Vì không giống như câu a, xử lý theo phương pháp cặp (key là (A, B)), lúc này
                // key chỉ gồm có 1 item, do đó khi các key được gom chung trong quá trình
                // reduce sẽ bị thiếu mất các trường hợp đồng hiện ngược lại (BA). Do đó phải
                // tạo thành 2 cặp <key, value>
                for (Couple c : arrCouple) {
                    Couple c1 = new Couple(c.getItem2(), one);
                    context.write(c.getItem1(), c1);
                    IntWritable iItem2 = c.getItem2();
                    if (iItem2.get() != -1) { // Nếu cặp đối tượng đồng hiện đang xét là đồng hiện với item ảo (-1) thì
                                              // chỉ cần out ra 1 <key, value> là đủ
                        Couple c2 = new Couple(c.getItem1(), one);
                        context.write(iItem2, c2);
                    }
                }
            }
        }
    }

    /**
     * Lớp mở rộng từ Reducer với thiết lập <code>&lt;key, value&gt;</code> đầu vào
     * và <code>&lt;key, value&gt;</code> đầu ra của pha Reduce. Có sử dụng kỹ thuật
     * Stripes trong pha Mapper do đó trong pha Reducer phải giải quyết các bài toán
     * phức tạp :)))
     */
    public static class CoupleStripesReducer extends Reducer<IntWritable, Couple, NullWritable, ProbAB> {

        // Tạo biến key cho output là null. Vì mình không cần out gì ra key cả.
        // Tạo ở đây để không phải cấp phát/thu hồi bộ nhớ nhiều lần trong hàm reduce,
        // gây lãng phí tài nguyên
        NullWritable nw;

        @Override
        public void reduce(IntWritable key, Iterable<Couple> values, Context context)
                throws IOException, InterruptedException {

            // Biến lưu tổng của các lần đồng hiện giữa key và 1 item nào đó trong value
            Map<Integer, Integer> result = new HashMap<>();
            int iCountA = 0; // Biến đếm số lần xuất hiện của item trong tất cả các tran

            // Lần lượt duyệt qua mảng chứa các value là cặp (item, count)
            for (Couple val : values) {
                Integer iB = val.getItem1().get(); // Lấy item
                if (iB.intValue() != -1) { // Nếu item khác -1
                    if (result.containsKey(iB)) { // Nếu mảng đã chứa item đó
                        Integer iNew = result.get(iB).intValue() + val.getItem2().get(); // thì cộng giá trị đồng hiện
                                                                                         // của nó vào giá trị hiện hành
                        result.replace(iB, iNew); // thay thế giá trị mới vào mảng lưu thông tin
                    } else { // Nếu mảng lưu thông tin chưa có item đang xét
                        result.put(iB, val.getItem2().get()); // thì đơn giản là thêm nó vào
                    }
                } else { // nếu item là item ảo (-1) thì cộng giá trị count của nó vào biến đếm số lần
                         // xuất hiện của item ở key
                    iCountA += val.getItem2().get();
                }
            }

            IntWritable iwCountA = new IntWritable(iCountA); // Đổi kiểu dữ liệu cho biến đếm số lần xuất hiện của key

            // Sau khi cộng xong các đồng hiện, cũng như số lần xuất hiện của key, thì thêm
            // nó vào trong lớp Prob và xuất kết quả ra ngoài cho người dùng
            for (Entry<Integer, Integer> entry : result.entrySet()) {
                ProbAB prob = new ProbAB(new IntWritable(entry.getKey()), key,
                        new IntWritable(entry.getValue().intValue()), iwCountA);
                context.write(nw, prob); // Ghi kết quả
            }
        }
    }

    public static class TriadMapper extends Mapper<Object, Text, Triad, IntWritable> {

        // Tạo biến chứa giá trị 1
        // Tạo ở đây để không phải cấp phát bộ nhớ nhiều lần trong hàm map, gây lãng phí
        IntWritable one = new IntWritable(1);

        // Override lại hàm map
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Cắt văn bản đầu vào thành từng dòng, ứng với từng transaction, để xử lý
            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {

                // Trong một transaction, cắt lấy từng sản phẩm (item)
                String[] arrItems = sLine.split(" ");

                // Lấy số lượng các item đã cắt được
                int iLen = arrItems.length;

                // Khởi tạo mảng chứa các triad tạo ra
                // Dùng mảng để kiểm tra xem có tồn tại couple hay chưa
                ArrayList<Triad> arrTriad = new ArrayList<>();

                // Lần lượt duyệt qua các item
                // Tạo Triad đồng hiện giữa nó và các item phía sau
                for (int i = 0; i < iLen - 2; i++) {
                    for (int j = i + 1; j < iLen - 1; j++) {
                        for(int k = j + 1; k < iLen; k++){
                        // Không tạo triad có chứa 2 giá trị bằng nhau
                            if ((!arrItems[i].equals(arrItems[j])) && (!arrItems[j].equals(arrItems[k])) 
                                        &&(!arrItems[i].equals(arrItems[k]))) {
                                Triad tNew = new Triad(arrItems[i], arrItems[j], arrItems[k]); // Tạo triad
                                boolean bExist = false; // Biến cho biết đã tồn tại Triad hay chưa
                                for (Triad t : arrTriad) {
                                    if (t.equals(tNew)) {
                                        // Nếu có rồi thì ghi nhận lại và thoát, không thêm
                                        bExist = true;
                                        break;
                                    }
                                }
                                if (!bExist)
                                    arrTriad.add(tNew);// Nếu chưa có thì thêm triad vào mảng
                            }
                        }
                    }
                }
               
                for (Triad t : arrTriad) {
                    context.write(t, one);
                }
                
            }
        }
    }

    public static class TriadReducer extends Reducer<Triad, IntWritable, Triad, IntWritable> {

        // Tạo biến chứa kết quả của reduce
        // Tạo ở đây để không phải cấp phát/thu hồi bộ nhớ nhiều lần trong hàm reduce,
        // gây lãng phí tài nguyên
        IntWritable result = new IntWritable();

        @Override
        public void reduce(Triad key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Tạo biến chứa tổng các value
            int sum = 0;

            // Lần lượt duyệt qua mảng chứa các value có chung key đã được gộp lại
            // Cộng tất cả lại với nhau
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Chuyển đổi biến tổng thành kiểu dữ liệu phù hợp để ghi ra
            result.set(sum);
            context.write(key, result); // Ghi kết quả
        }
    }

    public static class TriadStripesMapper extends Mapper<Object, Text, Couple, Couple> {

        // Tạo biến chứa giá trị 1
        // Tạo ở đây để không phải cấp phát bộ nhớ nhiều lần trong hàm map, gây lãng phí
        IntWritable one = new IntWritable(1);

        // Override lại hàm map
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Cắt văn bản đầu vào thành từng dòng, ứng với từng transaction, để xử lý
            String[] arrLines = value.toString().split("\n");

            for (String sLine : arrLines) {

                // Trong một transaction, cắt lấy từng sản phẩm (item)
                String[] arrItems = sLine.split(" ");

                // Lấy số lượng các item đã cắt được
                int iLen = arrItems.length;

                // Khởi tạo mảng chứa các triad tạo ra
                // Dùng mảng để kiểm tra xem có tồn tại triad hay chưa
                ArrayList<Triad> arrTriad = new ArrayList<>();

                // Đối với từng cặp item tạo bộ ba với item ảo là -1, sau này sẽ dùng để đếm cặp items đó
                // có trong bao nhiêu tran
                for (int i = 0; i < iLen - 1; i++) {
                    for (int j = i + 1; j < iLen; j++) {
                        // Không tạo triad chứa ít nhất 2 giá trị giống nhau
                        if (!arrItems[i].equals(arrItems[j])) {
                            Triad tNew = new Triad(arrItems[i], arrItems[j],"-1"); // Tạo triad với giá trị không tồn tại trong dữ li
                            boolean bExist = false; // Biến cho biết đã tồn tại triad hay chưa
                            for (Triad t : arrTriad) {
                                if (t.equals(tNew)) {
                                    // Nếu có rồi thì ghi nhận lại và thoát, không thêm
                                    bExist = true;
                                    break;
                                }
                            }
                            if (!bExist)
                                arrTriad.add(tNew);// Nếu chưa có thì thêm triad vào mảng
                        }
                    }
                }

                // Lần lượt duyệt qua các item
                // Tạo  đồng hiện giữa nó và các item phía sau
                for (int i = 0; i < iLen - 2; i++) {
                    for (int j = i + 1; j < iLen - 1; j++) {
                        for(int k = j + 1; k < iLen; k++){
                            // Không tạo triad chứa ít nhất 2 giá trị giống nhau
                            if ((!arrItems[i].equals(arrItems[j])) && (!arrItems[j].equals(arrItems[k])) 
                                        &&(!arrItems[i].equals(arrItems[k]))) {
                                Triad tNew = new Triad(arrItems[i], arrItems[j], arrItems[k]); // Tạo triad
                                boolean bExist = false; // Biến cho biết đã tồn tại triad hay chưa
                                for (Triad t : arrTriad) {
                                    if (t.equals(tNew)) {
                                        // Nếu có rồi thì ghi nhận lại và thoát, không thêm
                                        bExist = true;
                                        break;
                                    }
                                }
                                if (!bExist)
                                    arrTriad.add(tNew);// Nếu chưa có thì thêm triad vào mảng
                            }
                        }
                    }
                }
                /*
                Vì dữ liệu data các mã sản phẩm xếp theo thứ tự tăng dần trong từng dòng A<B<C
                nên 1 triad chỉ cần ghi 3 key couple AB, AC, BC 
                Ghi dưới dạng: key là couple (item1, item2) 
                             value là couple {item3 : 1} 
                             nếu item3 là -1 thì là đếm đồng hiện (item1, item2)

                 */
                for (Triad t : arrTriad) {
                    Couple c1 = new Couple(t.getItem3(),one);
                    context.write(new Couple(t.getItem1(),t.getItem2()),c1);  //(item1,item2){item3: one}
                    if(t.getItem3().get() != -1){  //item3 chỉ được dùng trong couple khóa khi có giá trị khác -1
                        Couple c2 = new Couple(t.getItem2(),one);
                        context.write(new Couple(t.getItem1(),t.getItem3()), c2); //(item1,item3){item2: one}
                        Couple c3 = new Couple(t.getItem1(),one);
                        context.write(new Couple(t.getItem2(),t.getItem3()), c3);//(item2,item3){item1: one}
                    }
                }
                
            }
        }
    }
    public static class TriadStripesReducer extends Reducer<Couple, Couple, NullWritable, ProbABC> {

        // Tạo biến key cho output là null. Vì mình không cần out gì ra key cả.
        // Tạo ở đây để không phải cấp phát/thu hồi bộ nhớ nhiều lần trong hàm reduce,
        // gây lãng phí tài nguyên
        NullWritable nw;

        @Override
        public void reduce(Couple key, Iterable<Couple> values, Context context)
                throws IOException, InterruptedException {

            // Biến lưu tổng của các lần đồng hiện giữa key và 1 item nào đó trong value
            Map<Integer, Integer> result = new HashMap<>();
            int iBC = 0;
            //duyệt các giá trị trong couple value(item3: count)
            for (Couple couple : values) {
              Integer iA = couple.getItem1().get(); 
              if (iA.intValue() != -1) {      //nếu item 3 # -1 => đếm đồng hiện 3 sản phẩm  
                if (result.containsKey(iA)) { //Nếu đã tồn tại item3 trước đó => chỉ cần cộng thêm giá trị count
                  Integer iANew = (Integer)result.get(iA) + couple.getItem2().get();
                  result.replace(iA, iANew); //Lưu lại giá trị
                  continue;
                } 
                result.put(iA, couple.getItem2().get()); //Nếu chưa tồn tại thì thêm vào
                continue;
              } 
              iBC += couple.getItem2().get(); //Nếu item 3 = -1 thì đang đếm đồng hiện item1 và item2
            } 
            //Chuyển sang kiểu dữ liệu phù hợp
            IntWritable iwCountBC = new IntWritable(iBC);
            for (Map.Entry<Integer, Integer> entry : result.entrySet()) {
                // Key là couple ở đây xem là B và C, value couple(A, count) 
                //Tạo triad để đưa về thứ tự A,B,C cho dễ nhớ  
              Triad triad = new Triad(entry.getKey().toString(), key.getItem1().toString(), key.getItem2().toString());
              //Truyền tham số vào hàm tính xác suất, tham số lần lượt (A,B,C, countABC, countBC)
              ProbABC probABC = new ProbABC(triad.getItem1(),triad.getItem2(),triad.getItem3(), new IntWritable(((Integer)entry.getValue()).intValue()), iwCountBC);
              context.write(this.nw, probABC);
            } 
          }
        }
    }


