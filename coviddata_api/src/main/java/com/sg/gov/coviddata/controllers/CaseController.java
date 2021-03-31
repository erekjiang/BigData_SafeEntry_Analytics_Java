package com.sg.gov.coviddata.controllers;


import com.sg.gov.coviddata.dto.CaseRecordDto;
import com.sg.gov.coviddata.util.Util;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
public class CaseController {

    @PostMapping("/case")
    CaseRecordDto save(@RequestBody CaseRecordDto recordDto){
        recordDto.setCaseId(UUID.randomUUID().toString());
        return recordDto;
    }

    @GetMapping("/cases")
    List<CaseRecordDto> get(@RequestParam LocalDateTime datetime){

        return IntStream.range(0,5).mapToObj( i -> {

            LocalDate diagnoseDt = Util.getRandomDiagnosedDt();
            LocalDateTime createdDt =  LocalDateTime.of(diagnoseDt,Util.randomTime());
            return CaseRecordDto.builder().
                    caseId(UUID.randomUUID().toString())
                    .active(true)
                    .activeStatus("IN_COMMUNITY_FACILITY")
                    .admittedDt(LocalDate.now())
                    .age(20)
                    .birthDt(Util.getRandomBirthDt())
                    .createdDttm(createdDt)
                    .hospitalizedHospital("SGH")
                    .importedCase(true)
                    .nric("S12213"+ i+"G")
                    .nationality("Singapore")
                    .race("Chinese")
                    .name("name" + i)
                    .sex("Male")
                    .passType("EP")
                    .deceasedDt(null)
                    .diagnosedDt(diagnoseDt)
                    .dischargedDt(null)
                    .importedFromCountry("India")
                    .lastupdatedDttm(LocalDateTime.now()).build();

        }).collect(Collectors.toList());
    }

}
