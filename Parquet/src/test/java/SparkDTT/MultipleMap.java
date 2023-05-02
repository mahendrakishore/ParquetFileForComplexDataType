package SparkDTT;

import java.util.ArrayList;
import java.util.List;

public class MultipleMap {

    public static void main(String[] args) {

    }

    private List<List<String>> getDataForFile() {
        List<List<String>> data = new ArrayList<>();

        List<String> parquetFileItem1 = new ArrayList<>();
        parquetFileItem1.add("1");
        parquetFileItem1.add("Name1");
        parquetFileItem1.add("true");

        List<String> parquetFileItem2 = new ArrayList<>();
        parquetFileItem2.add("2");
        parquetFileItem2.add("Name2");
        parquetFileItem2.add("false");

        data.add(parquetFileItem1);
        data.add(parquetFileItem2);

        return data;
    }
}


