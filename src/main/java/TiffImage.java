import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Created By @author songc
 * on 2017/11/3
 */
public class TiffImage {
    private byte[] content;
    private String name;

    public TiffImage(byte[] content, String name) {
        this.content = content;
        this.name = name;
    }

    public TiffImage(HbaseFile hbaseFile) {
        this.content = hbaseFile.getContent();
        this.name = hbaseFile.getRowKey();
    }
    public BufferedImage getImage() {
        try {
            return ImageIO.read(new ByteArrayInputStream(content));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public BufferedImage convert2Gray() {
        return ImageUtil.convert2Gray(this.getImage());
    }
}
