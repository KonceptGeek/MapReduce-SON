package cmpt741.project.models;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class ItemSet implements Comparable<ItemSet>{
    private List<Item> items;
    private int support;

    public ItemSet(List<Item> items, int support) {
        super();
        this.items = items;
        this.support = support;

        Collections.sort(this.items);
    }

    @Override
    public int compareTo(ItemSet otherItemSet) {
        List<Item> thisItems = this.getItems();
        List<Item> otherItems = otherItemSet.getItems();

        if (thisItems.equals(otherItems)) {
            return 0;
        }

        for (int index = 0; index < thisItems.size(); index++) {
            int diff = thisItems.get(index).compareTo(otherItems.get(index));
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    public void setItems(List<Item> items) {
        this.items = items;
    }

    public List<Item> getItems() {
        return items;
    }

    public void setSupport(int support) {
        this.support = support;
    }

    public int getSupport() {
        return this.support;
    }

    @Override
    public String toString() {
        String result = "";
        for (Item item : items) {
            result += item.getItemName();
            result = result + " ";
        }
        return result.trim();
    }
}
