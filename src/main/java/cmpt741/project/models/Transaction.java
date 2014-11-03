package cmpt741.project.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Transaction {
    private List<Item> items;
    private int transactionId;

    public void addItem(Item item) {
        if (items == null) {
            items = new ArrayList<>();
        }
        items.add(item);
    }

    public List<Item> getItems() {
        if (items != null) {
            return items;
        } else {
            return new ArrayList<>();
        }
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public int getTransactionId() {
        return this.transactionId;
    }

    @Override
    public String toString() {
        return "TransactionID: " + String.valueOf(transactionId) + ", Items: " + Arrays.toString(items.toArray());
    }
}
