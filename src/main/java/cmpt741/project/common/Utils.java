package cmpt741.project.common;

import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Utils {

    public static int getLinesInHadoopFile(Path path, FileSystem fileSystem) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        String line;
        int lineCount = 0;
        while((line = bufferedReader.readLine()) != null) {
            lineCount++;
        }
        return lineCount;
    }

    public static List<Integer> getIntegerArray(String line) {
        List<Integer> result = new ArrayList<>();
        String[] lineSplit = line.split("\\s+");
        for (String splitVal : lineSplit) {
            result.add(Integer.valueOf(splitVal.trim()));
        }
        return result;
    }

    public static String intArrayToString(List<Integer> array) {
        String result = "";
        for (Integer element : array) {
            result += String.valueOf(element) + " ";
        }
        return result.trim();
    }
}


