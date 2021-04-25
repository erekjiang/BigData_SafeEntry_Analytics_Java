package com.nus.iss.bd.dto;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown=true)
public class CaseRecordDto {

    private String caseId;
    private String nric;
    private Enums.PassType passType;
    private String nationality;
    private String race;
    private String name;
//    private LocalDate birthDt;
    private int age;
    private String sex;
//    private LocalDate diagnosedDt;
    private boolean active;
    private Enums.ActiveStatus activeStatus;
    private boolean importedCase;
    private String importedFromCountry;
    private String hospitalizedHospital;
//    private LocalDate admittedDt;
    private String dischargedDt;
    private boolean deceased;
//    private LocalDateTime deceasedDt;
//    private LocalDateTime createdDttm;
//    private LocalDateTime lastUpdatedDttm;

}
