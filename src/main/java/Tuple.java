import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Hashtable;

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
        StringBuilder s = new StringBuilder();
        for (Comparable name : entries.keySet()) {
            String key = name.toString();
            String value = entries.get(name).toString();
            s.append(key + " : " + value + ", ");
        }
        return "Tuple{" +
                "clusteringKey=" + clusteringKey.toString() +
                ", entries=" + entries.toString() +
                '}';
    }

}
