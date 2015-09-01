package examples;

import org.mapdb.*;

import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;

/*
 * Demonstrates how-to use multi value keys in BTree.
 *
 * MapDB has sortable tuples in form of an array. They allow multi--keys in ordinary TreeMap.
 * Values are sorted hierarchically,
 * fully indexed query must start on first value and continue on second, third and so on.
 */
public class TreeMap_Composite_Key {


    /*
     * In this example we demonstrate spatial queries on a Map
     * filled with Address > Income pairs.
     *
     * Address is represented as three-value-tuple.
     * First value is Town, second is Street and
     * third value is House number
     *
     * Java Generics are buggy, so we left out some type annotations for simplicity.
     * I would recommend more civilized language with type inference such as Kotlin or Scala.
     */
    @SuppressWarnings("rawtypes")
	public static void main(String[] args) {


        //initial values
        String[] towns = {"Galway", "Ennis", "Gort", "Cong", "Tuam"};
        String[] streets = {"Main Street", "Shop Street", "Second Street", "Silver Strands"};
        int[] houseNums = {1,2,3,4,5,6,7,8,9,10};

        DB db = DBMaker.memoryDB().make();
        //initialize map
        // note that it uses KeyArray Serialier to minimise disk space used by Map
        BTreeKeySerializer keySerializer = new BTreeKeySerializer.ArrayKeySerializer(
                new Comparator[]{Fun.COMPARATOR, Fun.COMPARATOR, Fun.COMPARATOR},
                new Serializer[]{Serializer.STRING, Serializer.STRING, Serializer.INTEGER}
        ) ;

        ConcurrentNavigableMap<Object[], Integer> map =
                db.treeMapCreate("test")
                    .keySerializer(keySerializer)
                    .make();


        //fill with values, use simple permutation so we dont have to include large test data.
        Random r = new Random(41);
        for(String town:towns)
            for(String street:streets)
                for(int houseNum:houseNums){
                    Object[] address = new Object[]{town, street, houseNum};
                    int income = r.nextInt(50000);
                    map.put(address, income);
                }

        System.out.println("There are "+map.size()+ " houses in total");  //NOTE: map.size() traverses entire map


        //Lets get all houses in Cong
        //Values are sorted so we can query sub-range (values between lower and upper bound)
        Map<Object[], Integer>
                housesInCong = map.subMap(
                new Object[]{"Cong"}, //shorter array is 'negative infinity'; all larger arrays are larger
                new Object[]{"Cong",null,null} // 'null' is 'positive infinity'; everything else is smaller then 'null'
        );

        System.out.println("There are "+housesInCong.size()+ " houses in Cong");

        //lets make sum of all salary in Cong
        int total = 0;
        for(Integer salary:housesInCong.values()){
            total+=salary;
        }
        System.out.println("Salary sum for Cong is: "+total);


        //Now different query, lets get total salary for all living in town center on 'Main Street', including all towns
        //We could iterate over entire map to get this information, but there is more efficient way.
        //Lets iterate over 'Main Street' in all towns.
        total = 0;
        for(String town:towns){

			Map<Object[], Integer> mainStreetHouses =
                    map.subMap(
                            new Object[]{town, "Main Street"}, //use missing value as LOWEST boundary for house number
                            new Object[]{town, "Main Street", null} // 'null' is HIGHEST boundary for house number
                    );
            for(Integer salary:mainStreetHouses.values()){
                total+=salary;
            }
        }
        System.out.println("Salary sum for all Main Streets is: "+total);


        //other example, lets remove Ennis/Shop Street from our DB
        map.subMap(
                new Object[]{"Ennis", "Shop Street"},
                new Object[]{"Ennis", "Shop Street", null})
                .clear();
    }
}

