package src.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


/*
- Tạo class mới làm value nên chỉ phải kế thừa từ lớp Writable
- Khi kế thừa, các hàm bắt buộc phải override lại là:
    + Constructors
    + readFields
    + write
    + toString
 */
/**
 * Lớp chứa dữ liệu xác suất mua sản phẩm A khi đã mua sản phẩm B, dùng làm value
 */
public class ProbAB implements Writable {
    private IntWritable iA; // Sản phẩm A
    private IntWritable iB; // Sản phẩm B
    private IntWritable countAB; // Số lần đồng hiện của cặp (A, B) trong các transaction
    private IntWritable countB; // Số lần xuất hiện của B trong các transaction (số trans mua B)

    // Constructor
    public ProbAB(IntWritable iA, IntWritable iB, IntWritable countAB, IntWritable countB) {
        this.iA = iA;
        this.iB = iB;
        this.countAB = countAB;
        this.countB = countB;
    }

    /**
     * Hàm ghi dữ liệu
     */
    public void write(DataOutput dataOutput) throws IOException {
        iA.write(dataOutput);
        iB.write(dataOutput);
        countAB.write(dataOutput);
        countB.write(dataOutput);
    }

    /**
     * Hàm đọc dữ liệu
     */
    public void readFields(DataInput dataInput) throws IOException {
        iA.readFields(dataInput);
        iB.readFields(dataInput);
        countAB.readFields(dataInput);
        countB.readFields(dataInput);
    }

    /**
     * Hàm chuyển class sang String, phục vụ cho việc ghi kết quả xuống, sau pha reduce
     * @return Trả về String có dạng <code>Prob(A|B) = Count(A,B) / Count(B) = số / số = bao nhiêu %</code>
     */
    @Override
    public String toString() {
        double dProbAB = ((double) (countAB.get())) / ((double) (countB.get())) * 100;
        return ("Prob(" + iA.get() + "|" + iB.get() + ") = Count(" + iA.get() + "," + iB.get() + ") / Count(" + iB.get()
                + ") = " + countAB.get() + " / " + countB.get() + " = " + dProbAB + "%");
    }
}
