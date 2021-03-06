import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;

/**
 * Created By @author songc
 * on 2017/11/3
 */
public class ImageUtil {

    public static BufferedImage convert2Gray(BufferedImage bufferedImage) {
        BufferedImage grayImage = new BufferedImage(bufferedImage.getWidth(), bufferedImage.getHeight(), BufferedImage.TYPE_BYTE_GRAY);
        for (int i = 0; i < bufferedImage.getWidth(); i++) {
            for (int j = 0; j < bufferedImage.getHeight(); j++) {
                final int color = bufferedImage.getRGB(i, j);
                final int r = (color >> 16) & 0xff;
                final int g = (color >> 8) & 0xff;
                final int b = color & 0xff;
                int gray = (int) (0.3 * r + 0.59 * g + 0.11 * b);
                int newPixel = colorToRGB(255, gray, gray, gray);
                grayImage.setRGB(i, j, newPixel);
            }
        }
        return grayImage;
    }

    public static double getRegionGrayAverage(BufferedImage image, int startX, int startY, int width, int height) {
        int[] data = new int[width * height];
        data = image.getRGB(startX, startY, width, height, data, 0, width);
        int sum = 0;
        for (int i : data) {
            sum += i & 0xff;
        }
        return sum / data.length;
    }

    /**
     * 对矩阵进行转置
     *
     * @param doubles 需要转置的矩阵
     * @return 转置后的矩阵
     */
    public static List<double[]> getTranspose(List<double[]> doubles) {
        int len = doubles.size();
        int lenArray = doubles.get(0).length;
        List<double[]> result = new ArrayList<>();
        for (int i = 0; i < lenArray; i++) {
            double[] array = new double[len];
            int j = 0;
            for (double[] aDouble : doubles) {
                array[j++] = aDouble[i];
            }
            result.add(array);
        }
        return result;
    }

    private static int colorToRGB(int alpha, int red, int green, int blue) {

        int newPixel = 0;
        newPixel += alpha;
        newPixel = newPixel << 8;
        newPixel += red;
        newPixel = newPixel << 8;
        newPixel += green;
        newPixel = newPixel << 8;
        newPixel += blue;
        return newPixel;
    }
}
