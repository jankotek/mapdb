package org.mapdb;

public class Serialized2DerivedBean extends Serialization2Bean {
    private static final long serialVersionUID = 2071817382135925585L;

    private String d1 = "1";
    private String d2 = "2";
    private String d3 = null;
    private String d4 = "4";
    private String d5 = null;
    private String d6 = "6";

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((d1 == null) ? 0 : d1.hashCode());
        result = prime * result + ((d2 == null) ? 0 : d2.hashCode());
        result = prime * result + ((d3 == null) ? 0 : d3.hashCode());
        result = prime * result + ((d4 == null) ? 0 : d4.hashCode());
        result = prime * result + ((d5 == null) ? 0 : d5.hashCode());
        result = prime * result + ((d6 == null) ? 0 : d6.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        Serialized2DerivedBean other = (Serialized2DerivedBean) obj;
        if (d1 == null) {
            if (other.d1 != null)
                return false;
        } else if (!d1.equals(other.d1))
            return false;
        if (d2 == null) {
            if (other.d2 != null)
                return false;
        } else if (!d2.equals(other.d2))
            return false;
        if (d3 == null) {
            if (other.d3 != null)
                return false;
        } else if (!d3.equals(other.d3))
            return false;
        if (d4 == null) {
            if (other.d4 != null)
                return false;
        } else if (!d4.equals(other.d4))
            return false;
        if (d5 == null) {
            if (other.d5 != null)
                return false;
        } else if (!d5.equals(other.d5))
            return false;
        if (d6 == null) {
            if (other.d6 != null)
                return false;
        } else if (!d6.equals(other.d6))
            return false;
        return true;
    }



}