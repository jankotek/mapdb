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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Serializer which handles POJO, object graphs etc.
 *
 * @author  Jan Kotek
 */
public class SerializerPojo extends SerializerBase implements Serializable{


    protected static final Serializer<CopyOnWriteArrayList<ClassInfo>> serializer = new Serializer<CopyOnWriteArrayList<ClassInfo>>() {

        @Override
		public void serialize(DataOutput out, CopyOnWriteArrayList<ClassInfo> obj) throws IOException {
            Utils.packInt(out, obj.size());
            for (ClassInfo ci : obj) {
                out.writeUTF(ci.getName());
                out.writeBoolean(ci.isEnum);
                out.writeBoolean(ci.useObjectStream);
                if(ci.useObjectStream) continue; //no fields

                Utils.packInt(out, ci.fields.size());
                for (FieldInfo fi : ci.fields) {
                    out.writeUTF(fi.getName());
                    out.writeBoolean(fi.isPrimitive());
                    out.writeUTF(fi.getType());
                }
            }
        }

        @Override
		public CopyOnWriteArrayList<ClassInfo> deserialize(DataInput in, int available) throws IOException{
            if(available==0) return new CopyOnWriteArrayList<ClassInfo>();

            int size = Utils.unpackInt(in);
            ArrayList<ClassInfo> ret = new ArrayList<ClassInfo>(size);

            for (int i = 0; i < size; i++) {
                String className = in.readUTF();
                boolean isEnum = in.readBoolean();
                boolean isExternalizable = in.readBoolean();

                int fieldsNum = isExternalizable? 0 : Utils.unpackInt(in);
                FieldInfo[] fields = new FieldInfo[fieldsNum];
                for (int j = 0; j < fieldsNum; j++) {
                    fields[j] = new FieldInfo(in.readUTF(), in.readBoolean(), in.readUTF(), classForName(className));
                }
                ret.add(new ClassInfo(className, fields,isEnum,isExternalizable));
            }
            return new CopyOnWriteArrayList<ClassInfo>(ret);
        }
    };

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(CC.FAIR_LOCKS);

    protected static Class<?> classForName(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    protected DB db;


    public SerializerPojo(CopyOnWriteArrayList<ClassInfo> registered){
        if(registered == null)
            registered = new CopyOnWriteArrayList<ClassInfo>();
        this.registered = registered;
        oldSize = registered.size();
        for(int i=0;i<registered.size();i++)
        {
            ClassInfo ci = registered.get(i);
            Class clazz = classForName(ci.getName());
            class2classId.put(clazz, i);
            classId2class.put(i, clazz);

        }
    }

    protected void setDb(DB db) {
        this.db = db;
    }

    /**
     * Stores info about single class stored in MapDB.
     * Roughly corresponds to 'java.io.ObjectStreamClass'
     */
    protected static final class ClassInfo {

        protected final String name;
        protected final List<FieldInfo> fields = new ArrayList<FieldInfo>();
        protected final Map<String, FieldInfo> name2fieldInfo = new HashMap<String, FieldInfo>();
        protected final Map<String, Integer> name2fieldId = new HashMap<String, Integer>();
        protected ObjectStreamField[] objectStreamFields;

        protected final boolean isEnum;

        protected final boolean useObjectStream;

        public ClassInfo(final String name, final FieldInfo[] fields, final boolean isEnum, final boolean isExternalizable) {
            this.name = name;
            this.isEnum = isEnum;
            this.useObjectStream = isExternalizable;

            for (FieldInfo f : fields) {
                this.name2fieldId.put(f.getName(), this.fields.size());
                this.fields.add(f);
                this.name2fieldInfo.put(f.getName(), f);
            }
        }

        public String getName() {
            return name;
        }

        public FieldInfo[] getFields() {
            return fields.toArray(new FieldInfo[fields.size()]);
        }

        public FieldInfo getField(String name) {
            return name2fieldInfo.get(name);
        }

        public int getFieldId(String name) {
            Integer fieldId = name2fieldId.get(name);
            if(fieldId != null)
                return fieldId;
            return -1;
        }

        public FieldInfo getField(int serialId) {
            return fields.get(serialId);
        }

        public int addFieldInfo(FieldInfo field) {
            name2fieldId.put(field.getName(), fields.size());
            name2fieldInfo.put(field.getName(), field);
            fields.add(field);
            return fields.size() - 1;
        }

        public ObjectStreamField[] getObjectStreamFields() {
            return objectStreamFields;
        }

        public void setObjectStreamFields(ObjectStreamField[] objectStreamFields) {
            this.objectStreamFields = objectStreamFields;
        }

        @Override public String toString(){
            return super.toString()+ "["+getName()+"]";
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

        public FieldInfo(String name, boolean primitive, String type, Class<?> clazz) {
            this.name = name;
            this.primitive = primitive;
            this.type = type;
            this.clazz = clazz;
            try {
                this.typeClass = primitive?null:Class.forName(type);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

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


        public FieldInfo(ObjectStreamField sf, Class<?> clazz) {
            this(sf.getName(), sf.isPrimitive(), sf.getType().getName(), clazz);
        }

        public String getName() {
            return name;
        }

        public boolean isPrimitive() {
            return primitive;
        }

        public String getType() {
            return type;
        }

        protected String firstCharCap(String s) {
            return Character.toUpperCase(s.charAt(0)) + s.substring(1);
        }
    }


    protected CopyOnWriteArrayList<ClassInfo> registered;
    protected Map<Class<?>, Integer> class2classId = new HashMap<Class<?>, Integer>();
    protected Map<Integer, Class<?>> classId2class = new HashMap<Integer, Class<?>>();



    public void registerClass(Class<?> clazz) throws IOException {
        if(clazz != Object.class)
            assertClassSerializable(clazz);

        if (containsClass(clazz))
            return;

        final boolean advancedSer = usesAdvancedSerialization(clazz);
        ObjectStreamField[] streamFields = advancedSer? new ObjectStreamField[0]:getFields(clazz);
        FieldInfo[] fields = new FieldInfo[streamFields.length];
        for (int i = 0; i < fields.length; i++) {
            ObjectStreamField sf = streamFields[i];
            fields[i] = new FieldInfo(sf, clazz);
        }

        ClassInfo i = new ClassInfo(clazz.getName(), fields,clazz.isEnum(), advancedSer);
        class2classId.put(clazz, registered.size());
        classId2class.put(registered.size(), clazz);
        registered.add(i);

        saveClassInfo();
    }

    protected boolean usesAdvancedSerialization(Class<?> clazz) {
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

    /** action performed after classInfo was modified, feel free to override */
    protected void saveClassInfo() {

    }

    protected ObjectStreamField[] getFields(Class<?> clazz) {
        ObjectStreamField[] fields = null;
        ClassInfo classInfo = null;
        Integer classId = class2classId.get(clazz);
        if (classId != null) {
            classInfo = registered.get(classId);
            fields = classInfo.getObjectStreamFields();
        }
        if (fields == null) {
            ObjectStreamClass streamClass = ObjectStreamClass.lookup(clazz);
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
            if(classInfo != null)
                classInfo.setObjectStreamFields(fields);
        }
        return fields;
    }

    protected void assertClassSerializable(Class<?> clazz) throws NotSerializableException, InvalidClassException {
        if(containsClass(clazz))
            return;

        if (!Serializable.class.isAssignableFrom(clazz))
            throw new NotSerializableException(clazz.getName());

    }

    public Object getFieldValue(String fieldName, Object object) {
        try {
            registerClass(object.getClass());
        } catch (IOException e) {
            throw new IOError(e);
        }
        ClassInfo classInfo = registered.get(class2classId.get(object.getClass()));
        return getFieldValue(classInfo.getField(fieldName), object);
    }

    public Object getFieldValue(FieldInfo fieldInfo, Object object) {

        if(fieldInfo.field==null){
            throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.getName());
        }


        try {
            return fieldInfo.field.get(object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Could not get value from field", e);
        }
    }

    public void setFieldValue(String fieldName, Object object, Object value) {
        try {
            registerClass(object.getClass());
        } catch (IOException e) {
            throw new IOError(e);
        }
        ClassInfo classInfo = registered.get(class2classId.get(object.getClass()));
        setFieldValue(classInfo.getField(fieldName), object, value);
    }

    public void setFieldValue(FieldInfo fieldInfo, Object object, Object value) {
        if(fieldInfo.field==null)
            throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.getName());

        try{
           fieldInfo.field.set(object, value);
        } catch (IllegalAccessException e) {
           throw new RuntimeException("Could not set field value: ",e);
        }

    }

    public boolean containsClass(Class<?> clazz) {
        return (class2classId.get(clazz) != null);
    }

    public int getClassId(Class<?> clazz) {
        Integer classId = class2classId.get(clazz);
        if(classId != null) {
            return classId;
        }
        throw new Error("Class is not registered: " + clazz);
    }

    @Override
    protected Engine getEngine() {
        return db.getEngine();
    }

    @Override
    protected void serializeUnknownObject(DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        if(db!=null){
            //check for named objects
            String name = db.getNameForObject(obj);
            if(name!=null){
                out.write(Header.NAMED);
                out.writeUTF(name);
                //TODO object stack here?
                return;
            }
        }
        out.write(Header.POJO);
        lock.writeLock().lock(); //TODO write lock is not necessary over entire method
        try{
            registerClass(obj.getClass());

            //write class header
            int classId = getClassId(obj.getClass());
            Utils.packInt(out, classId);
            ClassInfo classInfo = registered.get(classId);

            if(classInfo.useObjectStream){
                ObjectOutputStream2 out2 = new ObjectOutputStream2((OutputStream) out);
                out2.writeObject(obj);
                return;
            }


            if(classInfo.isEnum) {
                int ordinal = ((Enum<?>)obj).ordinal();
                Utils.packInt(out, ordinal);
            }

            ObjectStreamField[] fields = getFields(obj.getClass());
            Utils.packInt(out, fields.length);

            for (ObjectStreamField f : fields) {
                //write field ID
                int fieldId = classInfo.getFieldId(f.getName());
                if (fieldId == -1) {
                    //field does not exists in class definition stored in db,
                    //propably new field was added so add field descriptor
                    fieldId = classInfo.addFieldInfo(new FieldInfo(f, obj.getClass()));
                    saveClassInfo();
                }
                Utils.packInt(out, fieldId);
                //and write value
                Object fieldValue = getFieldValue(classInfo.getField(fieldId), obj);
                serialize(out, fieldValue, objectStack);
            }
        }finally{
            lock.writeLock().unlock();
        }
    }


    @Override
    protected Object deserializeUnknownHeader(DataInput in, int head, FastArrayList<Object> objectStack) throws IOException {
        if(head == Header.NAMED){
            String name = in.readUTF();
            Object o = db.get(name);
            if(o==null) throw new InternalError("Named object was not found: "+name);
            objectStack.add(o);
            return o;
        }

        if(head!= Header.POJO) throw new InternalError();

        lock.readLock().lock();
        //read class header
        try {
            int classId = Utils.unpackInt(in);
            ClassInfo classInfo = registered.get(classId);
//            Class clazz = Class.forName(classInfo.getName());
            Class<?> clazz = classId2class.get(classId);
            if(clazz == null)
                clazz = Class.forName(classInfo.getName());
            assertClassSerializable(clazz);

            Object o;

            if(classInfo.useObjectStream){
                ObjectInputStream2 in2 = new ObjectInputStream2((InputStream) in);
                o = in2.readObject();
            }else if(classInfo.isEnum) {
                int ordinal = Utils.unpackInt(in);
                o = clazz.getEnumConstants()[ordinal];
            }
            else{
                o = createInstanceSkippinkConstructor(clazz);
            }

            objectStack.add(o);

            if(!classInfo.useObjectStream){
                int fieldCount = Utils.unpackInt(in);
                for (int i = 0; i < fieldCount; i++) {
                    int fieldId = Utils.unpackInt(in);
                    FieldInfo f = classInfo.getField(fieldId);
                    Object fieldValue = deserialize(in, objectStack);
                    setFieldValue(f, o, fieldValue);
                }
            }
            return o;
        } catch (Exception e) {
            throw new Error("Could not instantiate class", e);
        }finally {
            lock.readLock().unlock();
        }
    }


    static protected Method sunConstructor = null;
    static protected Object sunReflFac = null;
    static protected Method androidConstructor = null;
    static private Method androidConstructorGinger = null;
    static private int constructorId;

    static{
        try{
            Class clazz = Class.forName("sun.reflect.ReflectionFactory");
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
            constructorId = (Integer) getConstructorId.invoke(null, Object.class);

            Method newInstance = ObjectStreamClass.class.getDeclaredMethod("newInstance", Class.class, int.class);
            newInstance.setAccessible(true);
            androidConstructorGinger = newInstance;

        }catch(Exception e){
            //ignore
        }
    }


    protected static Map<Class<?>, Constructor<?>> class2constuctor = new ConcurrentHashMap<Class<?>, Constructor<?>>();

    /**
     * For pojo serialization we need to instanciate class without invoking its constructor.
     * There are two ways to do it:
     * <p/>
     *   Using proprietary API on Oracle JDK and OpenJDK
     *   sun.reflect.ReflectionFactory.getReflectionFactory().newConstructorForSerialization()
     *   more at http://www.javaspecialists.eu/archive/Issue175.html
     * <p/>
     *   Using 'ObjectInputStream.newInstance' on Android
     *   http://stackoverflow.com/a/3448384
     * <p/>
     *   If non of these works we fallback into usual reflection which requires an no-arg constructor
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


        protected ObjectOutputStream2(OutputStream out) throws IOException, SecurityException {
            super(out);
        }

        @Override
        protected void writeClassDescriptor(ObjectStreamClass desc) throws IOException {
            Integer classId = class2classId.get(desc.forClass());
            if(classId ==null){
                registerClass(desc.forClass());
                classId = class2classId.get(desc.forClass());
            }
            Utils.packInt(this,classId);
        }
    }

    protected final class ObjectInputStream2 extends ObjectInputStream{

        protected ObjectInputStream2(InputStream in) throws IOException, SecurityException {
            super(in);
        }

        @Override
        protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
            Integer classId = Utils.unpackInt(this);
            Class clazz = classId2class.get(classId);
            return ObjectStreamClass.lookup(clazz);
        }
    }

    protected int oldSize;

    public boolean hasUnsavedChanges(){
        return oldSize!=registered.size();
    }
    public void save(Engine e){
        //TODO thread safe?
        e.update(Engine.CLASS_INFO_RECID, registered, SerializerPojo.serializer);
        oldSize = registered.size();
    }
}