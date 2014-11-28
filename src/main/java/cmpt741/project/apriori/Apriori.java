package cmpt741.project.apriori;

/*
Author: Zobayer Hasan
*/
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Apriori extends Thread{
    public static boolean debugger = false;

    private final Database db;
    private final List< Integer > itemset;
    private final List< List< Integer > > frequent;
    private double minsup;

    public Apriori(String thrName, Database db, double minsup) {
        super(thrName);
        this.db = db;
        itemset = db.getItemset();
        frequent = new ArrayList< List< Integer > >();
        this.minsup = minsup;
    }

    @Override
    public void run() {
        double startTime = System.currentTimeMillis();

        int k = 1, n = db.dbSize();
        List< List< Integer > > Ck = new ArrayList< List< Integer > >();
        List< List< Integer > > Lk = new ArrayList< List< Integer > >();
        HashMap< List< Integer>, Integer > seenK = new HashMap< List< Integer >, Integer >();

        for(Integer item : itemset) {
            List< Integer > temp = new ArrayList< Integer >();
            temp.add(item);
            Lk.add(temp);
        }

        while(k <= n && !Lk.isEmpty()) {
            if(debugger) {
                System.out.println("Step " + k);
                System.out.println("Lk: " + Lk);
            }

            seenK.clear();
            Ck.clear();
            for(List< Integer > kth : Lk) {
                int count = db.scanDatabase(kth);
                if((double)count < minsup) {
                    continue;
                }
                Ck.add(kth);
            }

            if(debugger) {
                System.out.println("Ck: " + Ck);
            }

            if(Ck.isEmpty()) break;

            for(List< Integer > freq : Ck) {
                frequent.add(freq);
                seenK.put(freq, k);
            }

            int[] prefixlen = new int[Ck.size()];
            prefixlen[0] = 0;
            for(int i = 1; i < Ck.size(); i++) {
                prefixlen[i] = prefixLen(Ck.get(i-1), Ck.get(i));
            }

            List< List< Integer > > temp = new ArrayList< List< Integer > >();
            for(int i = 0; i < Ck.size(); i++) {
                for(int j = i + 1; j < Ck.size(); j++) {
                    if(prefixlen[j] == k-1) {
                        if(debugger) {
                            System.out.println("Joining: " + i + ":" + Ck.get(i) + " + " + j + ":" + Ck.get(j) + " Prefix Length " + prefixlen[j]);
                        }
                        temp.add(prefixJoin(Ck.get(i), Ck.get(j)));
                    }
                    else break;
                }
            }

            if(debugger) {
                System.out.println("Temporary: " + temp);
            }

            Lk.clear();
            for(List< Integer > list : temp) {
                boolean candid = true;
                if(k > 1) {
                    for(int i = 0; i < list.size(); i++) {
                        List< Integer > prev = new ArrayList< Integer >();
                        for(int j = 0; j < list.size(); j++) {
                            if(i != j) prev.add(list.get(j));
                        }
                        if(!seenK.containsKey(prev)) {
                            candid = false;
                            break;
                        }
                    }
                }
                if(candid) {
                    Lk.add(list);
                }
            }

            if(debugger) {
                System.out.println("Pruned: " + Lk);
            }

            k++;
        }

        double endTime = System.currentTimeMillis();
        System.out.println("Apriori completed in " + (endTime - startTime)/1000.0 + " seconds");
    }

    public void printPatterns() {
        System.out.println("Frequent Itemsets");
        for(List< Integer > pattern : frequent) {
            System.out.println(pattern);
        }
        System.out.println("Total " + frequent.size() + " itemsets");
    }

    public List<List<Integer>> getFrequentItemsets() {
        return frequent;
    }

    private int prefixLen(List< Integer > left, List< Integer > right) {
        int len = 0;
        for(len = 0; len < left.size() && len < right.size(); len++) {
            if(left.get(len).compareTo(right.get(len)) != 0) return len;
        }
        return len;
    }

    private List< Integer > prefixJoin(List< Integer > left, List< Integer > right) {
        List< Integer > ret = new ArrayList< Integer >();
        for(Integer i : left) {
            ret.add(i);
        }
        ret.add(right.get(right.size() - 1));
        return ret;
    }
}