package com.nus.iss.bd.coviddataproducer.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CaseRecordDto {

    private String caseId;
    private String nric;
    private String passType;
    private String nationality;
    private String race;
    private String name;
    private LocalDate birthDt;
    private int age;
    private String sex;
    private LocalDate diagnosedDt;
    private boolean active;
    private String activeStatus;
    private boolean importedCase;
    private String importedFromCountry;
    private String hospitalizedHospital;
    private LocalDate admittedDt;
    private String dischargedDt;
    private LocalDateTime deceasedDt;
    private LocalDateTime createdDttm;
    private LocalDateTime lastupdatedDttm;

}
