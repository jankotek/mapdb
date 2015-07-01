package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Map;


public class cache_right_and_wrong {

    static class Person implements Cloneable{
        private String name;
        private int age;

        public void setName(String name) {
            this.name = name;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public Person clone(){
            Person ret = new Person();
            ret.age = age;
            ret.name = name;
            return ret;
        }
    }

    public static void main(String[] args) {

        DB db = DBMaker
                .memoryDB()
                .cacheHardRefEnable()
                .make();

        Map<String, Person> map =
                db.hashMap("map");


        //a
        //wrong
        Person person = new Person();
        map.put("John", person);
        person.setName("John");

        //right
        person = new Person();
        person.setName("John");
        map.put("John", person);

        //wrong
        person = map.get("John");
        person.setAge(15);

        //right, create copy which is modified and inserted
        person = map.get("John");
        person = person.clone(); //defensive copy
        person.setAge(15);
        map.put("John", person);
        //z
    }
}
