import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String name;
    private int i;
    private Vector<String> pageNames;
    private Hashtable<String, Comparable> minPageValue;
    private Hashtable<String, Vector<String>> overflow;

    public Table(String name) {
        this.name = name;
        i = 1;
        pageNames = new Vector<>();
        minPageValue = new Hashtable();
        overflow = new Hashtable<>();
    }

    public Hashtable<String, Vector<String>> getOverflow() {
        return overflow;
    }

    public String getName() {
        return name;
    }

    public int getI() {
        return i;
    }

    public void setI(int i) {
        this.i = i;
    }

    public Vector getPageNames() {
        return pageNames;
    }

    public Hashtable<String, Comparable> getMinPageValue() {
        return minPageValue;
    }


}
