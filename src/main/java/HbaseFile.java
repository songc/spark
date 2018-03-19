import java.io.Serializable;

/**
 * Created By @author songc
 * on 2017/11/4
 */
public class HbaseFile implements Serializable{
    private String rowKey;
    private byte[] content;

    public HbaseFile() {

    }

    public HbaseFile(String rowKey, Long parentId, String name, byte[] content) {
        this.rowKey = rowKey;
        this.content = content;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }
}
