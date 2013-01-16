package examples;

import org.mapdb.DBMaker;
import org.mapdb.Fun;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Demonstrates how-to use multi value keys in BTree.
 * <p/>
 * MapDB has `sortable tuples`. They allow multi value keys in ordinary TreeMap.
 * Values are sorted hierarchically,
 * fully indexed query must start on first value and continue on second, third and so on.
 */
public class TreeMap_Composite_Key {


    /**
     * In this example we demonstrate spatial queries on a Map
     * filled with Address > Income pairs.
     * <p/>
     * Address is represented as three-value-tuple.
     * First value is Town, second is Street and
     * third value is House number
     * <p/>
     * Java Generics are buggy, so we left out some type annotations for simplicity.
     * I would recommend more civilized language with type inference such as Kotlin or Scala.
     */
    @SuppressWarnings("rawtypes")
	public static void main(String[] args) {

        //initial values
        String[] towns = {"Galway", "Ennis", "Gort", "Cong", "Tuam"};
        String[] streets = {"Main street", "Shop street", "Second street", "Silver Strands"};
        int[] houseNums = {1,2,3,4,5,6,7,8,9,10};

        //initialize map
        ConcurrentNavigableMap<Fun.Tuple3, Integer> map =
                DBMaker.newTempTreeMap();


        //fill with values, use simple permutation so we dont have to include large test data.
        Random r = new Random(41);
        for(String town:towns)
            for(String street:streets)
                for(int houseNum:houseNums){
                    Fun.Tuple3<String, String, Integer> address = Fun.t3(town, street, houseNum);
                    int income = r.nextInt(50000);
                    map.put(address, income);
                }

        System.out.println("There are "+map.size()+ " houses in total");  //NOTE: map.size() traverses entire map


        //Lets get all houses in Cong
        //Values are sorted so we can query sub-range (values between lower and upper bound)
        Map<Fun.Tuple3, Integer>
                housesInCong = map.subMap(
                Fun.t3("Cong", null, null), //null is 'negative infinity'; everything else is larger than null
                Fun.t3("Cong", Fun.HI, Fun.HI) // 'HI' is 'positive infinity'; everything else is smaller then 'HI'
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
            @SuppressWarnings("unused")
			Map<Fun.Tuple3, Integer> mainStreetHouses =
                    map.subMap(
                            Fun.t3(town, "Main Street", null), //use null as LOWEST boundary for house number
                            Fun.t3(town, "Main Street", Fun.HI)
                    );
            for(Integer salary:housesInCong.values()){
                total+=salary;
            }
        }
        System.out.println("Salary sum for all Main Streets is: "+total);
        

        //other example, lets remove Ennis/Shop Street from our DB
        map.subMap(
                Fun.t3("Ennis", "Shop Street", null),
                Fun.t3("Ennis", "Shop Street", Fun.HI))
                .clear();


    }
}
