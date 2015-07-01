/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mapdb;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializer which handles POJO, object graphs etc.
 *
 * @author  Jan Kotek
 */
public class SerializerPojo extends SerializerBase implements Serializable{


    protected static final Serializer<ClassInfo> CLASS_INFO_SERIALIZER = new Serializer<ClassInfo>() {

        @Override
		public void serialize(DataOutput out, ClassInfo ci) throws IOException {
            out.writeUTF(ci.name);
            out.writeBoolean(ci.isEnum);
            out.writeBoolean(ci.useObjectStream);
            if(ci.useObjectStream)
                return; //no fields

            DataIO.packInt(out, ci.fields.length);
            for (FieldInfo fi : ci.fields) {
                out.writeUTF(fi.name);
                out.writeBoolean(fi.primitive);
                out.writeUTF(fi.type);
            }
        }

        @Override
		public ClassInfo deserialize(DataInput in, int available) throws IOException{
            final ClassLoader classLoader = SerializerPojo.classForNameClassLoader();

            String className = in.readUTF();
            boolean isEnum = in.readBoolean();
            boolean isExternalizable = in.readBoolean();

            int fieldsNum = isExternalizable? 0 : DataIO.unpackInt(in);
            FieldInfo[] fields = new FieldInfo[fieldsNum];
            for (int j = 0; j < fieldsNum; j++) {
                fields[j] = new FieldInfo(in.readUTF(), in.readBoolean(), classLoader, in.readUTF(), classForName(classLoader, className));
            }
            return new ClassInfo(className, fields,isEnum,isExternalizable);
        }

        @Override
        public boolean isTrusted() {
            return true;
        }


    };
    private static final long serialVersionUID = 3181417366609199703L;

    protected static ClassLoader classForNameClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    protected static Class<?> classForName(String className) {
        return classForName(classForNameClassLoader(), className);
    }

    protected static Class<?> classForName(ClassLoader loader, String className) {
        try {
            return Class.forName(className, true,loader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected final Engine engine;

    protected final Fun.Function1<String,Object> getNameForObject;
    protected final Fun.Function1<Object,String> getNamedObject;

    protected final Fun.Function0<ClassInfo[]> getClassInfos;
    protected final Fun.Function1Int<ClassInfo> getClassInfo;
    protected final Fun.Function1<Void,String> notifyMissingClassInfo;

    public SerializerPojo(
            Fun.Function1<String, Object> getNameForObject,
            Fun.Function1<Object, String> getNamedObject,
            Fun.Function1Int<ClassInfo> getClassInfo,
            Fun.Function0<ClassInfo[]> getClassInfos,
            Fun.Function1<Void, String> notifyMissingClassInfo,
            Engine engine){
        this.getNameForObject = getNameForObject;
        this.getNamedObject = getNamedObject;
        this.engine = engine;
        this.getClassInfo = getClassInfo!=null?getClassInfo:new Fun.Function1Int<ClassInfo>() {
            @Override public ClassInfo run(int a) {
                return null;
            }
        };
        this.getClassInfos = getClassInfos!=null?getClassInfos:new Fun.Function0<ClassInfo[]>() {
            @Override
            public ClassInfo[] run() {
                return new ClassInfo[0];
            }
        };
        this.notifyMissingClassInfo = notifyMissingClassInfo;
    }



    /**
     * Stores info about single class stored in MapDB.
     * Roughly corresponds to 'java.io.ObjectStreamClass'
     */
    protected static final class ClassInfo {

        //TODO optimize deserialization cost here.

        protected final String name;
        protected final FieldInfo[] fields;
        protected final Map<String, FieldInfo> name2fieldInfo = new HashMap<String, FieldInfo>();
        protected final Map<String, Integer> name2fieldId = new HashMap<String, Integer>();
        protected ObjectStreamField[] objectStreamFields;

        protected final boolean isEnum;

        protected final boolean useObjectStream;

        public ClassInfo(final String name, final FieldInfo[] fields, final boolean isEnum, final boolean isExternalizable) {
            this.name = name;
            this.isEnum = isEnum;
            this.useObjectStream = isExternalizable;

            this.fields = fields.clone();

            //TODO constructing dictionary might be contraproductive, perhaps use linear scan for smaller sizes
            for (int i=0;i<fields.length;i++) {
                FieldInfo f = fields[i];
                this.name2fieldId.put(f.name, i);
                this.name2fieldInfo.put(f.name, f);
            }
        }

        public int getFieldId(String name) {
            Integer fieldId = name2fieldId.get(name);
            if(fieldId != null)
                return fieldId;
            return -1;
        }

        public ObjectStreamField[] getObjectStreamFields() {
            return objectStreamFields;
        }


        @Override public String toString(){
            return super.toString()+ "["+name+"]";
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ClassInfo classInfo = (ClassInfo) o;

            if (isEnum != classInfo.isEnum) return false;
            if (useObjectStream != classInfo.useObjectStream) return false;
            if (name != null ? !name.equals(classInfo.name) : classInfo.name != null) return false;
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            return Arrays.equals(fields, classInfo.fields);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (fields != null ? Arrays.hashCode(fields) : 0);
            result = 31 * result + (isEnum ? 1 : 0);
            result = 31 * result + (useObjectStream ? 1 : 0);
            return result;
        }
    }

    /**
     * Stores info about single field stored in MapDB.
     * Roughly corresponds to 'java.io.ObjectFieldClass'
     */
    protected static class FieldInfo {
        protected final String name;
        protected final boolean primitive;
        protected final String type;
        protected Class<?> typeClass;
        // Class containing this field
        protected final Class<?> clazz;
        protected Field field;

        FieldInfo(String name, boolean primitive, String type, Class<?> clazz) {
            this(name, primitive, SerializerPojo.classForNameClassLoader(), type, clazz);
        }

        public FieldInfo(String name, boolean primitive, ClassLoader classLoader, String type, Class<?> clazz) {
            this(name, type, primitive ? null : classForName(classLoader, type), clazz);
        }

        public FieldInfo(ObjectStreamField sf, ClassLoader loader, Class<?> clazz) {
            this(sf.getName(), sf.isPrimitive(), loader, sf.getType().getName(), clazz);
        }

        public FieldInfo(String name, String type, Class<?> typeClass, Class<?> clazz) {
            this.name = name;
            this.primitive = typeClass == null;
            this.type = type;
            this.clazz = clazz;
            this.typeClass = typeClass;

            //init field

            Class<?> aClazz = clazz;

            // iterate over class hierarchy, until root class
            while (true) {
                if(aClazz == Object.class) throw new RuntimeException("Could not set field value: "+name+" - "+clazz.toString());
                // access field directly
                try {
                    Field f = aClazz.getDeclaredField(name);
                    // security manager may not be happy about this
                    if (!f.isAccessible())
                        f.setAccessible(true);
                    field = f;
                    break;
                } catch (NoSuchFieldException e) {
                    //field does not exists
                }
                // move to superclass
                aClazz = aClazz.getSuperclass();
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldInfo fieldInfo = (FieldInfo) o;

            if (primitive != fieldInfo.primitive) return false;
            if (name != null ? !name.equals(fieldInfo.name) : fieldInfo.name != null) return false;
            if (type != null ? !type.equals(fieldInfo.type) : fieldInfo.type != null) return false;
            if (typeClass != null ? !typeClass.equals(fieldInfo.typeClass) : fieldInfo.typeClass != null) return false;
            if (clazz != null ? !clazz.equals(fieldInfo.clazz) : fieldInfo.clazz != null) return false;
            return !(field != null ? !field.equals(fieldInfo.field) : fieldInfo.field != null);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (primitive ? 1 : 0);
            result = 31 * result + (type != null ? type.hashCode() : 0);
            result = 31 * result + (typeClass != null ? typeClass.hashCode() : 0);
            result = 31 * result + (clazz != null ? clazz.hashCode() : 0);
            result = 31 * result + (field != null ? field.hashCode() : 0);
            return result;
        }
    }




    public static ClassInfo makeClassInfo(ClassLoader classLoader, String className){
        Class clazz = classForName(classLoader, className);
        final boolean advancedSer = usesAdvancedSerialization(clazz);
        ObjectStreamField[] streamFields = advancedSer ? new ObjectStreamField[0] : makeFieldsForClass(clazz);
        FieldInfo[] fields = new FieldInfo[streamFields.length];
        for (int i = 0; i < fields.length; i++) {
            ObjectStreamField sf = streamFields[i];
            fields[i] = new FieldInfo(sf, classLoader, clazz);
        }

        return new ClassInfo(clazz.getName(), fields, clazz.isEnum(), advancedSer);
    }

    protected static boolean usesAdvancedSerialization(Class<?> clazz) {
        if(Externalizable.class.isAssignableFrom(clazz)) return true;
        try {
            if(clazz.getDeclaredMethod("readObject",ObjectInputStream.class)!=null) return true;
        } catch (NoSuchMethodException e) {
        }
        try {
            if(clazz.getDeclaredMethod("writeObject",ObjectOutputStream.class)!=null) return true;
        } catch (NoSuchMethodException e) {
        }


        try {
            if(clazz.getDeclaredMethod("writeReplace")!=null) return true;
        } catch (NoSuchMethodException e) {
        }

        return false;
    }


    protected static ObjectStreamField[] fieldsForClass(ClassInfo[] classes, Class<?> clazz) {
        ObjectStreamField[] fields = null;
        ClassInfo classInfo = null;
        int classId = classToId(classes,clazz.getName());
        if (classId != -1) {
            classInfo = classes[classId];
            fields = classInfo.getObjectStreamFields();
        }
        if (fields == null) {
            fields = makeFieldsForClass(clazz);
        }
        return fields;
    }

    private static ObjectStreamField[] makeFieldsForClass(Class<?> clazz) {
        ObjectStreamField[] fields;ObjectStreamClass streamClass = ObjectStreamClass.lookup(clazz);
        FastArrayList<ObjectStreamField> fieldsList = new FastArrayList<ObjectStreamField>();
        while (streamClass != null) {
            for (ObjectStreamField f : streamClass.getFields()) {
                fieldsList.add(f);
            }
            clazz = clazz.getSuperclass();
            streamClass = ObjectStreamClass.lookup(clazz);
        }
        fields = new ObjectStreamField[fieldsList
                .size];
        System.arraycopy(fieldsList.data, 0, fields, 0, fields.length);
        //TODO what is StreamField? perhaps performance optim?
//        if(classInfo != null)
//            classInfo.setObjectStreamFields(fields);
        return fields;
    }

    public boolean isSerializable(Object o){
        if(super.isSerializable(o))
            return true;

        return Serializable.class.isAssignableFrom(o.getClass());
    }

    protected void assertClassSerializable(ClassInfo[] classes, Class<?> clazz) throws NotSerializableException, InvalidClassException {
        if(classToId(classes,clazz.getName())!=-1)
            return;

        if (!Serializable.class.isAssignableFrom(clazz))
            throw new NotSerializableException(clazz.getName());

    }


    public Object getFieldValue(FieldInfo fieldInfo, Object object) {

        if(fieldInfo.field==null){
            throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.name);
        }


        try {
            return fieldInfo.field.get(object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not get value from field", e);
        }
    }



    public void setFieldValue(FieldInfo fieldInfo, Object object, Object value) {
        if(fieldInfo.field==null)
            throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.name);

        try{
           fieldInfo.field.set(object, value);
        } catch (IllegalAccessException e) {
           throw new RuntimeException("Could not set field value: ",e);
        }

    }


    public static int classToId(ClassInfo[] classes, String className) {
        for(int i=0;i<classes.length;i++){
            if(classes[i].name.equals(className))
                return i;
        }
        return -1;
    }

    @Override
    protected Engine getEngine() {
        return engine;
    }

    @Override
    protected void serializeUnknownObject(DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        if(getNameForObject!=null){
            //check for named objects
            String name = getNameForObject.run(obj);
            if(name!=null){
                out.write(Header.NAMED);
                out.writeUTF(name);
                //TODO object stack here?
                return;
            }
        }

        out.write(Header.POJO);

        ClassInfo[] classes = getClassInfos.run();
        assertClassSerializable(classes,obj.getClass());
        //write class header
        int classId = classToId(classes,obj.getClass().getName());
        if(classId==-1){
            //unknown class, fallback into object OutputOutputStream
            DataIO.packInt(out,-1);
            ObjectOutputStream2 out2 = new ObjectOutputStream2((OutputStream) out, classes);
            out2.writeObject(obj);
            //and notify listeners about missing class
            if(notifyMissingClassInfo!=null)
                notifyMissingClassInfo.run(obj.getClass().getName());
            return;
        }




        Class<?> clazz = obj.getClass();
        if( !clazz.isEnum() && clazz.getSuperclass()!=null && clazz.getSuperclass().isEnum())
            clazz = clazz.getSuperclass();

        if(clazz != Object.class)
            assertClassSerializable(classes,clazz);


        //write class header
        DataIO.packInt(out, classId);
        ClassInfo classInfo = classes[classId];

        if(classInfo.useObjectStream){
            ObjectOutputStream2 out2 = new ObjectOutputStream2((OutputStream) out, classes);
            out2.writeObject(obj);
            return;
        }


        if(classInfo.isEnum) {
            int ordinal = ((Enum<?>)obj).ordinal();
            DataIO.packInt(out, ordinal);
        }

        ObjectStreamField[] fields = fieldsForClass(classes, clazz);
        DataIO.packInt(out, fields.length);

        for (ObjectStreamField f : fields) {
            //write field ID
            int fieldId = classInfo.getFieldId(f.getName());
            if (fieldId == -1) {
                throw new AssertionError("Missing field: "+f.getName());
                //TODO class info is immutable in 2.0, so this old code can not be used
//                //field does not exists in class definition stored in db,
//                //probably new field was added so add field descriptor
//                fieldId = classInfo.addFieldInfo(new FieldInfo(f, clazz));
//                saveClassInfo();
            }
            DataIO.packInt(out, fieldId);
            //and write value
            Object fieldValue = getFieldValue(classInfo.fields[fieldId], obj);
            serialize(out, fieldValue, objectStack);
        }
    }


    @Override
    protected Object deserializeUnknownHeader(DataInput in, int head, FastArrayList<Object> objectStack) throws IOException {
        if(head == Header.NAMED){
            String name = in.readUTF();
            Object o = getNamedObject.run(name);
            if(o==null)
                throw new DBException.DataCorruption("Named object was not found: "+name);
            objectStack.add(o);
            return o;
        }

        if(head!= Header.POJO)
            throw new DBException.DataCorruption("wrong header");
        try{
            int classId = DataIO.unpackInt(in);
            ClassInfo classInfo = getClassInfo.run(classId);

            //is unknown Class or uses specialized serialization
            if(classId==-1 || classInfo.useObjectStream){
                //deserialize using object stream
                ObjectInputStream2 in2 = new ObjectInputStream2(in, getClassInfos.run());
                Object o = in2.readObject();
                objectStack.add(o);
                return o;
            }

            Class<?> clazz = classForNameClassLoader().loadClass(classInfo.name);
            if (!Serializable.class.isAssignableFrom(clazz))
                throw new NotSerializableException(clazz.getName());

            Object o;
            if(classInfo.isEnum) {
                int ordinal = DataIO.unpackInt(in);
                o = clazz.getEnumConstants()[ordinal];
            }
            else{
                o = createInstanceSkippinkConstructor(clazz);
            }

            objectStack.add(o);


            int fieldCount = DataIO.unpackInt(in);
            for (int i = 0; i < fieldCount; i++) {
                int fieldId = DataIO.unpackInt(in);
                FieldInfo f = classInfo.fields[fieldId];
                Object fieldValue = deserialize(in, objectStack);
                setFieldValue(f, o, fieldValue);
            }

            return o;
        } catch (Exception e) {
            throw new RuntimeException("Could not instantiate class", e);
        }
    }


    static protected Method sunConstructor = null;
    static protected Object sunReflFac = null;
    static protected Method androidConstructor = null;
    static private Method androidConstructorGinger = null;
    static private Object constructorId;

    static{
        try{
            Class<?> clazz = classForName("sun.reflect.ReflectionFactory");
            if(clazz!=null){
                Method getReflectionFactory = clazz.getMethod("getReflectionFactory");
                sunReflFac = getReflectionFactory.invoke(null);
                sunConstructor = clazz.getMethod("newConstructorForSerialization",
                        java.lang.Class.class, java.lang.reflect.Constructor.class);
            }
        }catch(Exception e){
            //ignore
        }

        if(sunConstructor == null)try{
            //try android way
            Method newInstance = ObjectInputStream.class.getDeclaredMethod("newInstance", Class.class, Class.class);
            newInstance.setAccessible(true);
            androidConstructor = newInstance;

        }catch(Exception e){
            //ignore
        }

        //this method was taken from 
        //http://dexmaker.googlecode.com/git-history/5a7820356e68a977711afc854d6cd71296c56391/src/mockito/java/com/google/dexmaker/mockito/UnsafeAllocator.java
        //Copyright (C) 2012 The Android Open Source Project, licenced under Apache 2 license
        if(sunConstructor == null && androidConstructor == null)try{
            //try android post ginger way
            Method getConstructorId = ObjectStreamClass.class.getDeclaredMethod("getConstructorId", Class.class);
            getConstructorId.setAccessible(true);
            constructorId = getConstructorId.invoke(null, Object.class);

            Method newInstance = ObjectStreamClass.class.getDeclaredMethod("newInstance", Class.class, getConstructorId.getReturnType());
            newInstance.setAccessible(true);
            androidConstructorGinger = newInstance;

        }catch(Exception e){
            //ignore
        }
    }


    protected static Map<Class<?>, Constructor<?>> class2constuctor = new ConcurrentHashMap<Class<?>, Constructor<?>>();

    /**
     * <p>
     * For pojo serialization we need to instantiate class without invoking its constructor.
     * There are two ways to do it:
     * </p><p>
     *   Using proprietary API on Oracle JDK and OpenJDK
     *   sun.reflect.ReflectionFactory.getReflectionFactory().newConstructorForSerialization()
     *   more at http://www.javaspecialists.eu/archive/Issue175.html
     * </p><p>
     *   Using {@code ObjectInputStream.newInstance} on Android
     *   http://stackoverflow.com/a/3448384
     * </p><p>
     *   If non of these works we fallback into usual reflection which requires an no-arg constructor
     * </p>
     */
    @SuppressWarnings("restriction")
	protected <T> T createInstanceSkippinkConstructor(Class<T> clazz)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {

        if(sunConstructor !=null){
            //Sun specific way
            Constructor<?> intConstr = class2constuctor.get(clazz);

            if (intConstr == null) {
                Constructor<?> objDef = Object.class.getDeclaredConstructor();
                intConstr = (Constructor<?>) sunConstructor.invoke(sunReflFac, clazz, objDef);
                class2constuctor.put(clazz, intConstr);
            }

            return (T)intConstr.newInstance();
        }else if(androidConstructor!=null){
            //android (harmony) specific way
            return (T)androidConstructor.invoke(null, clazz, Object.class);
        }else if(androidConstructorGinger!=null){
            //android (post ginger) specific way
            return (T)androidConstructorGinger.invoke(null, clazz, constructorId);
        }
        else{
            //try usual generic stuff which does not skip constructor
            Constructor<?> c = class2constuctor.get(clazz);
            if(c==null){
                c =clazz.getConstructor();
                if(!c.isAccessible()) c.setAccessible(true);
                class2constuctor.put(clazz,c);
            }
            return (T)c.newInstance();
        }
    }



    protected final class ObjectOutputStream2 extends ObjectOutputStream{

        private final ClassInfo[] classes;

        protected ObjectOutputStream2(OutputStream out, ClassInfo[] classes) throws IOException, SecurityException {
            super(out);
            this.classes = classes;
        }

        @Override
        protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
            int classId = classToId(classes,desc.getName());
            DataIO.packInt(this,classId);
            if(classId==-1){
                //unknown class, write its full name
                this.writeUTF(desc.getName());
                //and notify about unknown class
                if(notifyMissingClassInfo!=null)
                    notifyMissingClassInfo.run(desc.getName());
            }
        }
    }

    protected final class ObjectInputStream2 extends ObjectInputStream{

        private final ClassInfo[] classes;

        // One-element cache to handle the common case where we immediately resolve a descriptor to its class.
        // Unlike most ObjecTInputStream subclasses we actually have to look up the class to find the descriptor!
        private ObjectStreamClass lastDescriptor;
        private Class lastDescriptorClass;

        protected ObjectInputStream2(DataInput in, ClassInfo[] classes) throws IOException, SecurityException {
            super(new DataIO.DataInputToStream(in));
            this.classes = classes;
        }

        @Override
        protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
            int classId = DataIO.unpackInt(this);

            final Class clazz;
            if(classId == -1){
                //unknown class, so read its name
                String className = this.readUTF();
                clazz = Class.forName(className, false, SerializerPojo.classForNameClassLoader());
            }else{
                String className = classes[classId].name;
                clazz = SerializerPojo.classForNameClassLoader().loadClass(className);
            }
            final ObjectStreamClass descriptor = ObjectStreamClass.lookup(clazz);

            lastDescriptor = descriptor;
            lastDescriptorClass = clazz;

            return descriptor;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            if (desc == lastDescriptor) return lastDescriptorClass;

            ClassLoader loader = SerializerPojo.classForNameClassLoader();
            Class<?> clazz = Class.forName(desc.getName(), false, loader);
            if (clazz != null)
                return clazz;
            return super.resolveClass(desc);
        }
    }
}