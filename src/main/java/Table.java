import java.io.*;
import java.util.Properties;
import java.util.Vector;

public class Table implements Serializable {
    String name;
    // Vector<String> pages;
    int i;

    public Table(String name) {
        this.name = name;
        i = 1;
        //pages = new Vector<String>();

    }

    public void createPage() {
        Properties prop = new Properties();
        String fileName = "app.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {

        }
        try {
            prop.load(is);
        } catch (IOException ex) {
        }
        int N =Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        try {
            Vector<Object> page = new Vector<Object>();
            page.setSize(N);
            ObjectOutputStream o = new
                    ObjectOutputStream(
                    new FileOutputStream(name + i + ".bin"));
            i++;
            o.writeObject(page);
            o.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }

}
