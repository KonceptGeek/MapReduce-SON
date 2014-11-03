package cmpt741.project.models;

/**
 * @author Jasneet Sabharwal (jasneet.sabharwal@sfu.com)
 */
public class Item implements Comparable<Item>{
    private String itemName;

    public Item(String itemName) {
        this.itemName = itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public String getItemName() {
        return this.itemName;
    }

    @Override
    public int compareTo(Item o) {
        if (this.getItemName().equals(o.getItemName())) {
            return 0;
        } else {
            return this.getItemName().compareTo(o.getItemName());
        }
    }

    @Override
    public String toString() {
        return itemName;
    }
}
