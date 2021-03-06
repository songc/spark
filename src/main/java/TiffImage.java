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

    public double getRegionGrayAverage(int startX, int startY, int width, int height) {
        return ImageUtil.getRegionGrayAverage(this.convert2Gray(), startX, startY, width, height);
    }

    public double[] getAllRegionGrayAverage(int regionWidth, int regionHeight) {
        BufferedImage image = this.convert2Gray();
        int width = image.getWidth();
        int height = image.getHeight();
        int wNum = width / regionWidth;
        int hNum = height / regionHeight;
        double[] result = new double[wNum * hNum];
        for (int i = 0; i < wNum; i++) {
            for (int j = 0; j < hNum; j++) {
                result[i * hNum + j] = ImageUtil.getRegionGrayAverage(image, regionHeight * i, regionHeight * j, regionWidth, regionHeight);
            }
        }
        return result;
    }
}
