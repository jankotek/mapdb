package org.mapdb;

import java.io.Serializable;


public class Serialization2Bean implements Serializable {
    // =========================== Constants ===============================
    private static final long serialVersionUID = 2757814409580877461L;

    // =========================== Attributes ==============================
    private String id = "test";
    private String f1 = "";
    private String f2 = "";
    private String f3 = null;
    private String f4 = "";
    private String f5 = null;
    private String f6 = "";

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((f1 == null) ? 0 : f1.hashCode());
        result = prime * result + ((f2 == null) ? 0 : f2.hashCode());
        result = prime * result + ((f3 == null) ? 0 : f3.hashCode());
        result = prime * result + ((f4 == null) ? 0 : f4.hashCode());
        result = prime * result + ((f5 == null) ? 0 : f5.hashCode());
        result = prime * result + ((f6 == null) ? 0 : f6.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Serialization2Bean other = (Serialization2Bean) obj;
        if (f1 == null) {
            if (other.f1 != null) {
                return false;
            }
        } else if (!f1.equals(other.f1)) {
            return false;
        }
        if (f2 == null) {
            if (other.f2 != null) {
                return false;
            }
        } else if (!f2.equals(other.f2)) {
            return false;
        }
        if (f3 == null) {
            if (other.f3 != null) {
                return false;
            }
        } else if (!f3.equals(other.f3)) {
            return false;
        }
        if (f4 == null) {
            if (other.f4 != null) {
                return false;
            }
        } else if (!f4.equals(other.f4)) {
            return false;
        }
        if (f5 == null) {
            if (other.f5 != null) {
                return false;
            }
        } else if (!f5.equals(other.f5)) {
            return false;
        }
        if (f6 == null) {
            if (other.f6 != null) {
                return false;
            }
        } else if (!f6.equals(other.f6)) {
            return false;
        }
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }

}