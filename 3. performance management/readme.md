# Performance Management
1. debugging mode의 Chrome을 실행합니다.
2. Google 계정 로그인, Slack Workspace의 Analytics - 멤버 페이지에서 csv 파일을 다운로드합니다. 
3. 다운받은 csv 파일의 이름(실명)과 사용자 ID를 일일 공정 계획 엑셀 파일의 작업자(실명) 및 작업자 코드와 매칭한 후 tag에 알맞은 형태로 Transform합니다.
4. 해당 날짜의 작업물 분배 목록과 업로드 목록을 불러온 후, 업로드 개수와 재작업 여부를 검사합니다.
5. 검사 결과 재작업, 추가 업로드가 필요한 경우 Bot application이 재작업 공지 채널에 해당 작업자를 tag한 후 데이터셋 이름 및 재작업 사유를 전송합니다.
6. 해당 결과를 Bot application이 관리자용 채널에 작업자별 작업 현황을 전송합니다.
7. 완료 결과를 각각 xlsx, MySQL DB로 저장합니다.
8. 완료 결과 중 종별 수량을 확인합니다. 
9. Slack의 notice_me 채널에 작업 완료 알림을 전송합니다.
