import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlbumBean implements WritableComparable<AlbumBean> {
    private Text area;
    private IntWritable count;

    public AlbumBean() {
    }

    public AlbumBean(Text area, IntWritable count) {
        this.area = area;
        this.count = count;
    }

    public Text getArea() {
        return area;
    }

    public void setArea(Text area) {
        this.area = area;
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(IntWritable count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AlbumBean{" +
                "area=" + area +
                ", count=" + count +
                '}';
    }

    public int compareTo(AlbumBean o) {
        if (o == null) {
            throw new NullPointerException();
        }
        return -(this.count.compareTo(o.count));
    }

    /**
     * 序列化输出
     *
     * @param dataOutput
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(area.toString());
        dataOutput.writeInt(count.get());
    }

    /**
     * 序列化输入
     *
     * @param dataInput
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        area = new Text(dataInput.readUTF());
        count = new IntWritable(dataInput.readInt());
    }
}