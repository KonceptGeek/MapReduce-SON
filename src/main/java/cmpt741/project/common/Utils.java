package cmpt741.project.common;

import cmpt741.project.models.Item;
import cmpt741.project.models.ItemSet;
import cmpt741.project.models.Transaction;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Utils {

    public static List<Transaction> readTransactionsFromHadoop(Path filePath, String splitRegex, FileSystem fileSystem)
            throws IOException {
        List<Transaction> result = new ArrayList<>();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));

        String line;
        int transactionId = 0;
        while((line = bufferedReader.readLine()) != null) {
            Transaction transaction = new Transaction();
            String[] lineSplit = line.trim().split(splitRegex);
            for (String splitVal : lineSplit) {
                transaction.addItem(new Item(splitVal.trim()));
            }
            transaction.setTransactionId(transactionId++);
            result.add(transaction);
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

    public static int getLinesInHadoopFile(Path path, FileSystem fileSystem) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        String line;
        int lineCount = 0;
        while((line = bufferedReader.readLine()) != null) {
            lineCount++;
        }
        return lineCount;
    }

    public static ItemSet getItemset(String line) {
        String[] lineSplit = line.split("\\s+");
        List<Item> items = new ArrayList<>();
        for (String item : lineSplit) {
            items.add(new Item(item.trim()));
        }

        return new ItemSet(items, 0);
    }

    public static ItemSet getItemset(Set line) {
        String[] lineSplit = (String[]) line.toArray(new String[line.size()]);
        List<Item> items = new ArrayList<>();
        for (String item : lineSplit) {
            items.add(new Item(item.trim()));
        }

        return new ItemSet(items, 0);
    }
}


