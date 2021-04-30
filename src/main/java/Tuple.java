import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Hashtable;
import java.util.Vector;

public class Tuple implements Comparable, Serializable {
    private Comparable clusteringKey;
    private Hashtable<String, Comparable> entries;

    public Tuple(Comparable clusteringKey, Hashtable<String, Comparable> entries) {
        this.clusteringKey = clusteringKey;
        this.entries = entries;
    }


    public Comparable getClusteringKey() {
        return clusteringKey;
    }

    public Hashtable<String, Comparable> getEntries() {
        return entries;
    }

    public void setEntries(Hashtable<String, Comparable> ent) {
        this.entries = ent;
    }

    @Override
    public int compareTo(Object o) {
        return this.clusteringKey.compareTo(((Tuple) o).getClusteringKey());
    }

    @Override
    public String toString() {

        return "Tuple{" +
                "clusteringKey=" + clusteringKey.toString() +
                ", entries=" + entries.toString() +
                '}' + "\n";
    }

}
