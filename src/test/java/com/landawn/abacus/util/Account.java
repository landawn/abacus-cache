package com.landawn.abacus.util;

import java.sql.Timestamp;

import com.landawn.abacus.annotation.Column;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Account {

    private long id;
    private String gui;
    private String emailAddress;
    @Column("first_name")
    private String firstName;
    private String middleName;
    private String lastName;
    private Timestamp birthDate;
    private int status;
    private Timestamp lastUpdateTime;
    private Timestamp createTime;

}
