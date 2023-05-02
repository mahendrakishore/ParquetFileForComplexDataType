//package prac;
//
//import it.unimi.dsi.fastutil.Hash;
//import org.apache.spark.sql.sources.In;
//import scala.Char;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class NumCount {
//
//    public static void main(String[] args) {
//        String s = "chekc for Duplicate values";
//
//        Map<Character, Integer> m = new HashMap<Character,Integer>();
//        char[] c = s.toCharArray();
//        for(char cc: c){
//            if(m.containsKey(cc)){
//                m.put(cc,m.get(cc)+1);
//            }
//            else{
//                m.put(cc,1);
//            }
//        }
//
//        for(Map.Entry<Character,Integer> e: m.entrySet()){
//            System.out.println(e.getKey()+":"+e.getValue());
//        }
//
//    }
//}
