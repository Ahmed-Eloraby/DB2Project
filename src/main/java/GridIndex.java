import java.util.Collections;
import java.util.Vector;

public class GridIndex {
    private Vector<String> gridList;
    private Vector<String> columnNames;
    private Vector<Integer> noOfPossibleElements;
    private Vector<Comparable> minimumforColumn;

    public GridIndex(String tableName, String[] columnNames, Vector<Integer> minMaxComparison, Vector<Comparable> minimumforColumn) {
        this.gridList = new Vector<String>((int) Math.pow(11, columnNames.length));
        this.columnNames = new Vector<String>();
        this.noOfPossibleElements = new Vector<Integer>();
        for (int x : minMaxComparison) {
            noOfPossibleElements.addElement(x / 10 + 1);
        }
        this.minimumforColumn = minimumforColumn;
        Collections.addAll(this.columnNames, columnNames);
    }
}
