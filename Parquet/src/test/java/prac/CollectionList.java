package prac;

import org.apache.arrow.vector.holders.SmallIntHolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class CollectionList {
    public static void main(String[] args) {
        List<String> sl = Arrays.asList("one","two");
       // System.out.println(sl);

        Iterator<String> itr = sl.iterator();
        while(itr.hasNext()){
            System.out.println(itr.next());
        }
    }
}
