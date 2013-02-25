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
public class SerializerPojo extends SerializerBase{


    protected static final Serializer<CopyOnWriteArrayList<ClassInfo>> serializer = new Serializer<CopyOnWriteArrayList<ClassInfo>>() {

        @Override
		public void serialize(DataOutput out, CopyOnWriteArrayList<ClassInfo> obj) throws IOException {
            Utils.packInt(out, obj.size());
            for (ClassInfo ci : obj) {
                out.writeUTF(ci.getName());
                out.writeBoolean(ci.isEnum);
                out.writeBoolean(ci.isExternalizable);
                if(ci.isExternalizable) continue; //no fields

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

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static Class<?> classForName(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }




    public SerializerPojo(CopyOnWriteArrayList<ClassInfo> registered){
        if(registered == null)
            this.registered = new CopyOnWriteArrayList<ClassInfo>();
        else
            this.registered = registered;
    }

    /**
     * Stores info about single class stored in JDBM.
     * Roughly corresponds to 'java.io.ObjectStreamClass'
     */
    protected static class ClassInfo {

        private final String name;
        private final List<FieldInfo> fields = new ArrayList<FieldInfo>();
        private final Map<String, FieldInfo> name2fieldInfo = new HashMap<String, FieldInfo>();
        private final Map<String, Integer> name2fieldId = new HashMap<String, Integer>();
        private ObjectStreamField[] objectStreamFields;

        final boolean isEnum;

        final boolean isExternalizable;

        ClassInfo(final String name, final FieldInfo[] fields, final boolean isEnum, final boolean isExternalizable) {
            this.name = name;
            this.isEnum = isEnum;
            this.isExternalizable = isExternalizable;

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


    }

    /**
     * Stores info about single field stored in JDBM.
     * Roughly corresponds to 'java.io.ObjectFieldClass'
     */
    static class FieldInfo {
        private final String name;
        private final boolean primitive;
        private final String type;
        private Class<?> typeClass;
        // Class containing this field
        private final Class<?> clazz;
        private Object setter;
        private Object getter;

        public FieldInfo(String name, boolean primitive, String type, Class<?> clazz) {
            this.name = name;
            this.primitive = primitive;
            this.type = type;
            this.clazz = clazz;
            try {
                this.typeClass = Class.forName(type);
            } catch (ClassNotFoundException e) {
                this.typeClass = null;
            }
            initSetter();
            initGetter();
        }

        private void initSetter() {
            // Set setter
            String setterName = "set" + firstCharCap(name);

            Class<?> aClazz = clazz;

            // iterate over class hierarchy, until root class
            while (aClazz != Object.class) {
                // check if there is getMethod
                try {
                    Method m = aClazz.getMethod(setterName, typeClass);
                    if (m != null) {
                        setter = m;
                        return;
                    }
                } catch (Exception e) {
                    // e.printStackTrace();
                }

                // no get method, access field directly
                try {
                    Field f = aClazz.getDeclaredField(name);
                    // security manager may not be happy about this
                    if (!f.isAccessible())
                        f.setAccessible(true);
                    setter = f;
                    return;
                } catch (Exception e) {
//					e.printStackTrace();
                }
                // move to superclass
                aClazz = aClazz.getSuperclass();
            }
        }

        private void initGetter() {
            // Set setter
            String getterName = "get" + firstCharCap(name);

            Class<?> aClazz = clazz;

            // iterate over class hierarchy, until root class
            while (aClazz != Object.class) {
                // check if there is getMethod
                try {
                    Method m = aClazz.getMethod(getterName);
                    if (m != null) {
                        getter = m;
                        return;
                    }
                } catch (Exception e) {
                    // e.printStackTrace();
                }

                // no get method, access field directly
                try {
                    Field f = aClazz.getDeclaredField(name);
                    // security manager may not be happy about this
                    if (!f.isAccessible())
                        f.setAccessible(true);
                    getter = f;
                    return;
                } catch (Exception e) {
//					e.printStackTrace();
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

        private String firstCharCap(String s) {
            return Character.toUpperCase(s.charAt(0)) + s.substring(1);
        }
    }


    CopyOnWriteArrayList<ClassInfo> registered;
    Map<Class<?>, Integer> class2classId = new HashMap<Class<?>, Integer>();
    Map<Integer, Class<?>> classId2class = new HashMap<Integer, Class<?>>();



    public void registerClass(Class<?> clazz) throws IOException {
        if(clazz != Object.class)
            assertClassSerializable(clazz);

        if (containsClass(clazz))
            return;

        ObjectStreamField[] streamFields = getFields(clazz);
        FieldInfo[] fields = new FieldInfo[streamFields.length];
        for (int i = 0; i < fields.length; i++) {
            ObjectStreamField sf = streamFields[i];
            fields[i] = new FieldInfo(sf, clazz);
        }

        ClassInfo i = new ClassInfo(clazz.getName(), fields,clazz.isEnum(), Externalizable.class.isAssignableFrom(clazz));
        class2classId.put(clazz, registered.size());
        classId2class.put(registered.size(), clazz);
        registered.add(i);


        saveClassInfo();
    }

    /** action performed after classInfo was modified, feel free to override */
    protected void saveClassInfo() {

    }

    private ObjectStreamField[] getFields(Class<?> clazz) {
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
                    .size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fieldsList.get(i);
            }
            if(classInfo != null)
                classInfo.setObjectStreamFields(fields);
        }
        return fields;
    }

    private void assertClassSerializable(Class<?> clazz) throws NotSerializableException, InvalidClassException {
        if(containsClass(clazz))
            return;

        if (!Serializable.class.isAssignableFrom(clazz))
            throw new NotSerializableException(clazz.getName());
    }

    public Object getFieldValue(String fieldName, Object object) {
        try {
            registerClass(object.getClass());
        } catch (IOException e) {
            e.printStackTrace();
        }
        ClassInfo classInfo = registered.get(class2classId.get(object.getClass()));
        return getFieldValue(classInfo.getField(fieldName), object);
    }

    public Object getFieldValue(FieldInfo fieldInfo, Object object) {

        Object fieldAccessor = fieldInfo.getter;
        try {
            if (fieldAccessor instanceof Method) {
                Method m = (Method) fieldAccessor;
                return m.invoke(object);
            } else {
                Field f = (Field) fieldAccessor;
                return f.get(object);
            }
        } catch (Exception e) {

        }

        throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.getName());
    }

    public void setFieldValue(String fieldName, Object object, Object value) {
        try {
            registerClass(object.getClass());
        } catch (IOException e) {
            e.printStackTrace();
        }
        ClassInfo classInfo = registered.get(class2classId.get(object.getClass()));
        setFieldValue(classInfo.getField(fieldName), object, value);
    }

    public void setFieldValue(FieldInfo fieldInfo, Object object, Object value) {

        Object fieldAccessor = fieldInfo.setter;
        try {
            if (fieldAccessor instanceof Method) {
                Method m = (Method) fieldAccessor;
                m.invoke(object, value);
            } else {
                Field f = (Field) fieldAccessor;
                f.set(object, value);
            }
            return;
        } catch (Throwable e) {
            e.printStackTrace();
        }

        throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.getName());
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
    protected void serializeUnknownObject(DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        out.write(SerializationHeader.POJO);

        registerClass(obj.getClass());

        //write class header
        int classId = getClassId(obj.getClass());
        Utils.packInt(out, classId);
        ClassInfo classInfo = registered.get(classId);

        if(classInfo.isExternalizable){
            throw new InternalError("Can not serialize Externalizable class");
//            Externalizable o = (Externalizable) obj;
//            DataInputOutput out2 = (DataInputOutput) out;
//            try{
//                out2.serializer = this;
//                out2.objectStack = objectStack;
//                o.writeExternal(out2);
//            }finally {
//                out2.serializer = null;
//                out2.objectStack = null;
//            }
//            return;
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
    }


    @Override
    protected Object deserializeUnknownHeader(DataInput in, int head, FastArrayList<Object> objectStack) throws IOException {
        if(head!=SerializationHeader.POJO) throw new InternalError();

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

            if(classInfo.isEnum) {
                int ordinal = Utils.unpackInt(in);
                o = clazz.getEnumConstants()[ordinal];
            }
            else {
                o = createInstanceSkippinkConstructor(clazz);
            }

            objectStack.add(o);

            if(classInfo.isExternalizable){
                throw new InternalError("can not serialize Externalizable class");
//                Externalizable oo = (Externalizable) o;
//                DataInputOutput in2 = (DataInputOutput) in;
//                try{
//                    in2.serializer = this;
//                    in2.objectStack = objectStack;
//                    oo.readExternal(in2);
//                }finally {
//                    in2.serializer = null;
//                    in2.objectStack = null;
//                }

            }else{
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
            throw new Error("Could not instanciate class", e);
        }
    }


    static private Method sunConstructor = null;
    static private Object sunReflFac = null;
    static private Method androidConstructor = null;

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


    }


    private static Map<Class<?>, Constructor<?>> class2constuctor = new ConcurrentHashMap<Class<?>, Constructor<?>>();

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
        }else{
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

//    protected abstract Object deserialize(DataInput in, FastArrayList objectStack) throws IOException, ClassNotFoundException;

//    protected abstract void serialize(DataOutput out, Object fieldValue, FastArrayList objectStack) throws IOException;
//


}
