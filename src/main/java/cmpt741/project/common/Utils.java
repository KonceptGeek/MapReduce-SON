package cmpt741.project.common;

import cmpt741.project.models.Item;
import cmpt741.project.models.Transaction;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Utils {

    public static List<Transaction> readTransactionsFromFile(String filePath, String splitRegex)
            throws IOException {
        List<Transaction> result = new ArrayList<>();

        BufferedReader bufferedReader = new BufferedReader(
                new FileReader(new File(filePath)));

        String line;
        while((line = bufferedReader.readLine()) != null) {
            Transaction transaction = new Transaction();
            String[] lineSplit = line.trim().split(splitRegex);
            for (String splitVal : lineSplit) {
                transaction.addItem(new Item(splitVal.trim()));
            }
        }
        bufferedReader.close();
        return result;
    }

    /**
     * String to transaction.
     *
     * @param line
     * @param splitRegex
     * @return
     */
    public static Transaction getTransaction(String line, String splitRegex) {
        Transaction transaction = new Transaction();
        String[] lineSplit = line.split(splitRegex);
        for (String splitValue : lineSplit) {
            transaction.addItem(new Item(splitValue.trim()));
        }
        return transaction;
    }
}
