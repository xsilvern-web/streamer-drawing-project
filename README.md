# streamer-drawing-project
개인용 그림후원 프로그램 서버

## GIF 불러오기 회귀 검증

Node.js 18 이상에서 추가 패키지 없이 실행합니다.

```sh
node --test tests/gif-import.test.cjs
```

파일을 읽는 중 프레임 수가 2개에서 30개로 늘어나는 상황을 재현해,
전체 프레임 및 재생 시간 보존과 오류 발생 시 리소스 정리를 검증합니다.
브라우저 수동 검증은 Chrome 또는 Edge의 `/draw` 화면에서 이미지 불러오기로
GIF를 선택한 뒤 프레임 수와 각 프레임의 노출 시간을 확인합니다.
