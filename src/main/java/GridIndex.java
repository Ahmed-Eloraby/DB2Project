import java.util.Collections;
import java.util.Vector;

public class GridIndex {
    private Vector<String> gridList;
    private Vector<String> columnNames;
    private Vector<Vector<Comparable>> columnRanges;


    public GridIndex(String tableName, String[] columnNames,Vector<Vector<Comparable>> columnRanges) {
        this.gridList = new Vector<String>((int) Math.pow(11, columnNames.length));
        this.columnNames = new Vector<String>();
        this.columnRanges = columnRanges;
    }
}
