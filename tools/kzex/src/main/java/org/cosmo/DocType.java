package org.cosmo;

public enum DocType {
    STOCKS("stocks"),
    SALES("sales"),
    ;

    public final String name;

    DocType(String name) {
        this.name = name;
    }

    public static DocType fromName(String name) {
        for (DocType docType : values()) {
            if (docType.name.equalsIgnoreCase(name)) {
                return docType;
            }
        }
        throw new IllegalArgumentException("Unknown doc type: " + name);
    }
}
