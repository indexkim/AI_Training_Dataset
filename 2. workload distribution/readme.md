# Workload distribution
1. 일일 공정 계획 엑셀 파일에서 일자별 작업자 명단 및 작업자별 작업 수량을 불러옵니다.
2. 작업자 명단을 리스트로 변환한 후 작업 폴더를 생성합니다.
3. 작업자 명단과 작업자별 수량을 딕셔너리로 변환한 후, 작업물을 일괄 분배합니다.
4. 추가 분배 요청이 있는 경우 작업자와 작업 수량을 수동으로 설정한 후 분배합니다.
5. 분배 결과를 각각 xlsx, MySQL DB로 저장합니다.
6. 분배 결과 중 종별 수량을 확인합니다. 
7. Slack의 notice_me 채널에 작업 완료 후 알림을 전송합니다.
