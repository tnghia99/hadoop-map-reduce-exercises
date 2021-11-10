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
public class ProbABC implements Writable {
    private IntWritable iA; // Sản phẩm A
    private IntWritable iB; // Sản phẩm B
    private IntWritable iC; // Sản phẩm C
    private IntWritable countABC; // Số lần đồng hiện của bộ ba (A, B, C) trong các transaction
    private IntWritable countBC; // Số lần xuất hiện của cặp (B,C) trong các transaction (số trans mua cả B và C)

    // Constructor
    public ProbABC(IntWritable iA, IntWritable iB, IntWritable iC,IntWritable countABC, IntWritable countBC) {
        this.iA = iA;
        this.iB = iB;
        this.iC = iC;
        this.countABC = countABC;
        this.countBC = countBC;
    }

    /**
     * Hàm ghi dữ liệu
     */
    public void write(DataOutput dataOutput) throws IOException {
        iA.write(dataOutput);
        iB.write(dataOutput);
        iC.write(dataOutput);
        countABC.write(dataOutput);
        countBC.write(dataOutput);
    }

    /**
     * Hàm đọc dữ liệu
     */
    public void readFields(DataInput dataInput) throws IOException {
        iA.readFields(dataInput);
        iB.readFields(dataInput);
        iC.readFields(dataInput);
        countABC.readFields(dataInput);
        countBC.readFields(dataInput);
    }

    /**
     * Hàm chuyển class sang String, phục vụ cho việc ghi kết quả xuống, sau pha reduce
     * @return Trả về String có dạng <code>Prob(A|BC) = Count(A,B,C) / Count(BC) = số / số = bao nhiêu %</code>
     */
    @Override
    public String toString() {
        double dProbABC = ((double) (countABC.get())) / ((double) (countBC.get())) * 100;
        return ("Prob(" + iA.get() + "|" + iB.get() +","+ iC.get()+") = Count(" + iA.get() + "," + iB.get() + "," + iC.get() +") / Count(" + iB.get() + "," + iC.get()
                + ") = " + countABC.get() + " / " + countBC.get() + " = " + dProbABC + "%");
    }
}

