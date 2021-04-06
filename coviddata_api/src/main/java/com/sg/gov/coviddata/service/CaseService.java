package com.sg.gov.coviddata.service;


import com.sg.gov.coviddata.dto.CaseRecordDto;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class CaseService {

    private List<CaseRecordDto> caseRecordDtoList = new ArrayList<>();

    public List<CaseRecordDto> saveRecords(List<CaseRecordDto> data){
        List<CaseRecordDto> recordDtos = data.stream().map(record ->{
            record.setLastUpdatedDttm(LocalDateTime.now());
            record.setCaseId(UUID.randomUUID().toString());
            caseRecordDtoList.add(record);
            return record;
        }).collect(Collectors.toList());

        return recordDtos;
    }

    public List<CaseRecordDto> getCasesCreatedSinceTime(LocalDateTime dateTime){
        return caseRecordDtoList
                .stream()
                .filter(record -> record.getLastUpdatedDttm().isAfter(dateTime)).collect(Collectors.toList());
    }

}
