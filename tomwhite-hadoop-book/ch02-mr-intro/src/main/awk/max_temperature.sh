#!/usr/bin/env bash
for year in all/*
do
  echo -ne `basename $year .gz`"\t"
  # awk 스크립트로 기온, 특성 코드를 추출한 후 각 필드가 유효한 값을 가지는지 확인하고, 최고 기온을 구할 수 있도록 max 값 갱신
  gunzip -c $year | \
    awk '{ temp = substr($0, 88, 5) + 0;
           q = substr($0, 93, 1);
           if (temp !=9999 && q ~ /[01459]/ && temp > max) max = temp }
         END { print max }' # 파일의 모든 행이 처리된 후에 실행됨.
done