package cmpt741.project.singlemachine;

import cmpt741.project.apriori.Apriori;
import cmpt741.project.apriori.Database;

import java.io.*;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@gmail.com)
 */
public class AprioriTest {

    public static void main(String[] args) {

        Database db = null;
        try {
            db = new Database("1 4 8 9\n2 4 5\n1 3 8\n1 4 5\n2 5 7\n3 8 9");

        } catch(Exception e) {
            e.printStackTrace();
        }

        System.out.println("\nStarting Apriori");

        Apriori test1 = new Apriori("test1", db, 3);
        Apriori.debugger = false;
        test1.run();

            test1.printPatterns();

    }

    private static String readDataFromFile(String fileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
        String line;
        String result = "";
        while((line = reader.readLine()) != null) {
            result += line+"\\n";
        }
        return result;
    }

}
