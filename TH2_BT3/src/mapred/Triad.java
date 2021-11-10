package src.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/*
- Tạo class mới làm key nên phải kế thừa từ lớp WritableComparable
- Nếu chỉ cần tạo lớp làm value thì kế thừa lớp Writable là đủ
- Khi kế thừa, các hàm bắt buộc phải override lại là:
    + Constructors
    + readFields
    + write
    + toString
    + compareTo (để so sánh các key với nhau trong quá trình shuffle, sort)
 */
/**
 * Lớp chứa dữ liệu cặp sản phẩm đồng hiện dùng làm key
 */
public class Triad implements WritableComparable<Triad> {
    // Khởi tạo 3 item là 3 sản phẩm đồng hiện cùng nhau
    private IntWritable iItem1;
    private IntWritable iItem2;
    private IntWritable iItem3;

    /**
     * Hàm khởi tạo rỗng
     */
    public Triad() {
        iItem1 = new IntWritable(0);
        iItem2 = new IntWritable(0);
        iItem3 = new IntWritable(0);
    }

    /**
     * Hàm khởi tạo nhận giá trị IntWritable
     * 
     * @param iItem1 sản phẩm thứ nhất
     * @param iItem2 sản phẩm thứ hai
     * @param iItem3 sản phẩm thứ ba
     */
    public Triad(IntWritable iItem1, IntWritable iItem2, IntWritable iItem3) {
        this.iItem1 = iItem1;
        this.iItem2 = iItem2;
        this.iItem3 = iItem3;
    }

    /**
     * Hàm khởi tạo nhận vào chuỗi, rồi chuyển sang kiểu IntWritable
     * 
     * @param sItem1 sản phẩm thứ nhất
     * @param sItem2 sản phẩm thứ hai
     * @param sItem3 sản phẩm thứ ba
     */
    public Triad(String sItem1, String sItem2, String sItem3) {
        this.iItem1 = new IntWritable(Integer.parseInt(sItem1));
        this.iItem2 = new IntWritable(Integer.parseInt(sItem2));
        this.iItem3 = new IntWritable(Integer.parseInt(sItem3));
    }

    /**
     * Hàm ghi dữ liệu
     */
    public void write(DataOutput dataOutput) throws IOException {
        iItem1.write(dataOutput);
        iItem2.write(dataOutput);
        iItem3.write(dataOutput);
    }

    /**
     * Hàm đọc dữ liệu
     */
    public void readFields(DataInput dataInput) throws IOException {
        iItem1.readFields(dataInput);
        iItem2.readFields(dataInput);
        iItem3.readFields(dataInput);
    }

    /**
     * Hàm so sánh hai Triad.
     * 
     * @param otherTriad couple cần đem ra so sánh với Triad hiện tại
     * Các giá trị trong bộ ba item1 # item2 # item3 
     * => Triad1(A,B,C) = Triad2(X,Y,Z) khi A + B + C = X + Y + Z && A = X|Y|Z && B = X|Y|Z  
     *
     * 
     */
    @Override
    public int compareTo(Triad otherTriad) {
        if (((this.iItem1.get() + this.iItem2.get() + this.iItem3.get()) == (otherTriad.iItem1.get() + otherTriad.iItem2.get() + otherTriad.iItem3.get()))
                && ((this.iItem1.compareTo(otherTriad.iItem1) == 0)
                        || (this.iItem1.compareTo(otherTriad.iItem2) == 0)
                        || (this.iItem1.compareTo(otherTriad.iItem3)==0))
                && ((this.iItem2.compareTo(otherTriad.iItem1) == 0)
                        || (this.iItem2.compareTo(otherTriad.iItem2) == 0)
                        || (this.iItem2.compareTo(otherTriad.iItem3)==0))) {
            return 0;
        } else if (this.iItem1.get() < otherTriad.iItem1.get()) {
            return -1;
        } else if (this.iItem1.get() > otherTriad.iItem1.get()) {
            return 1;
        } else if (this.iItem2.get() < otherTriad.iItem2.get()) {
            return -1;
        } else if (this.iItem2.get() > otherTriad.iItem2.get()){
            return 1;
        } else if (this.iItem3.get() < otherTriad.iItem3.get()){
            return -1;
        }
        return 1;
    }



    /**
     * Hàm chuyển class sang String, phục vụ cho việc ghi kết quả xuống, sau pha
     * reduce
     * 
     * @return Trả về String có dạng <code>(item1 : item2)</code>
     */
    @Override
    public String toString() {
        return ("(" + this.iItem1.get() + " : " + this.iItem2.get() + " : " + this.iItem3.get() +")");
    }

    // Override lại hàm equals để so sánh các đối tượng Triad với nhau
    // Hàm thường được sử dụng để so sánh các key trong HashMap
    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (this.getClass() != o.getClass())
            return false;
        return (this.compareTo((Triad) o) == 0);
    }

    // Hàm hashCode thường phải được Override chung với hàm equals thì
    // hàm equals mới hoạt động được. Hai đối tượng equal nhau thì phải
    // có hashcode bằng nhau. Ngược lại thì không chắc!
    // Code trong phần comment là code chuẩn (classic way) của 1 hàm hashCode
    @Override
    public int hashCode() {
        /*
         * int result = 17; result = 31 * result + this.iItem1.hashCode(); result = 31 *
         * result + this.iItem2.hashCode(); return result;
         */
        return (this.iItem1.hashCode() + this.iItem2.hashCode() + this.iItem3.hashCode());
    }

    /**
     * Hàm trả về giá trị của item thứ nhất
     * 
     * @return Giá trị của Item 1 trong bộ ba (Item 1, Item 2, Item 3)
     */
    public IntWritable getItem1() {
        return this.iItem1;
    }

    /**
     * Hàm trả về giá trị của item thứ hai
     * 
     * @return Giá trị của Item 2 trong bộ ba (Item 1, Item 2, Item 3)
     */
    public IntWritable getItem2() {
        return this.iItem2;
    }

    /**
     * Hàm trả về giá trị của item thứ ba
     * 
     * @return Giá trị của Item 3 trong bộ ba (Item 1, Item 2, Item 3)
     */
    public IntWritable getItem3() {
        return this.iItem3;
    }
}
