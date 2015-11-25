package doc;

import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class dbmaker_atomicvar {

    static class Person{
        public static final Serializer<Person> SERIALIZER = new Serializer<Person>() {
            @Override
            public void serialize(DataOutput out, Person value) throws IOException {

            }

            @Override
            public Person deserialize(DataInput in, int available) throws IOException {
                return new Person();
            }
        } ;
    }

    public static void main(String[] args) {
        DB db = DBMaker
                .memoryDB()
                .make();
        //a
        Atomic.Var<Person> var = db.atomicVarCreate("mainPerson", null,  Person.SERIALIZER);
        //z
    }
}
