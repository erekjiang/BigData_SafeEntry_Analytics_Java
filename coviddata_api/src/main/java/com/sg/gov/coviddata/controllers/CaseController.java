package com.sg.gov.coviddata.controllers;


import com.sg.gov.coviddata.dto.CaseRecordDto;
import com.sg.gov.coviddata.dto.Enums;
import com.sg.gov.coviddata.service.CaseService;
import com.sg.gov.coviddata.util.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RestController
public class CaseController {

    @Autowired
    private CaseService caseService;

    @PostMapping("/case")
    CaseRecordDto save(@RequestBody CaseRecordDto recordDto){
        return caseService.saveRecords(Stream.of(recordDto).collect(Collectors.toList())).get(0);
    }

    @PostMapping("/cases")
    List<CaseRecordDto> save(@RequestBody List<CaseRecordDto> records){
        return caseService.saveRecords(records);
    }

    @GetMapping("/cases")
    List<CaseRecordDto> get(@RequestParam LocalDateTime datetime){
        return caseService.getCasesCreatedSinceTime(datetime);
    }

    @GetMapping("/random_cases")
    List<CaseRecordDto> get(){

        return IntStream.range(0,5).mapToObj( i -> {

            LocalDate diagnoseDt = Util.getRandomDiagnosedDt();
            LocalDateTime createdDt =  LocalDateTime.of(diagnoseDt,Util.randomTime());
            return CaseRecordDto.builder().
                    caseId(UUID.randomUUID().toString())
                    .active(true)
                    .activeStatus(Enums.ActiveStatus.HOSPITALIZED_STABLE)
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
                            .passType(Enums.PassType.EP)
                            .deceasedDt(null)
                            .diagnosedDt(diagnoseDt)
                            .dischargedDt(null)
                            .importedFromCountry("India")
                            .lastUpdatedDttm(LocalDateTime.now()).build();

        }).collect(Collectors.toList());
    }

}
