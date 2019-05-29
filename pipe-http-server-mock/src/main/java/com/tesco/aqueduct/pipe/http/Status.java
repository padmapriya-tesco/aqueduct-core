package com.tesco.aqueduct.pipe.http;

import lombok.Data;

@Data
public class Status {
    private final String status;

    public static Status ok() {
        return new Status("ok");
    }
}
