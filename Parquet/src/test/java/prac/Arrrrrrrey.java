package prac;

public class Arrrrrrrey {

    static int size = 3;
    static int count = 1;

    public static void main(String[] args) {

        int[] ia= new int[size];

for (int i=0;i<ia.length;i++){
    ia[i] = count;
    count++;
}
        for (int element: ia) {
            System.out.println(element);
        }
    }


}
