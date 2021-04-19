# OPENMOS
OpenMOS MTD with AAS v0.1 자산 관리 쉘 서버 개발 보관소

# 소개
이 프로젝트는 Eclipse [BaSyx](https://wiki.eclipse.org/BaSyx)를 기반으로 Industrie 4.0 AAS(Asset Administration Shell)를 구현 한 것입니다.

프로젝트는 아직 진행중이며, 다른 응용 프로그램, 설비(장치, 센서 등), 자산 관리 쉘 등 과의 표준화된 데이터 통신을 목적으로 합니다.

# 구조
![OpenMOS AAS Server Data Communication Architecture](https://user-images.githubusercontent.com/75360342/115129355-0fd5ce80-a020-11eb-864f-614c771c4c32.png)
OpenMOS AAS 서버는 다른 응용 프로그램, 설비, 자산 관리 쉘 등에서 데이터를 수신할 경우 어댑터를 통해 자산 관리 쉘 모델 구현 형식에 적합한 프로토콜로 변환합니다.

변환된 데이터를 통해 호출된 메서드를 실행하여 BaSyx SDK에 접근하며, BaSyx에 구현된 메서드를 통해 자산 관리 쉘을 구현합니다.

구현된 자산 관리 쉘은 클라이언트에서 요청할 경우 Json 유형으로 요청 값을 전송합니다.

BaSyx 기반으로 BaSyx 프로젝트의 메서드를 사용할 수 있으며, 데이터 통신 또한 VAB를 이용하며 이루어질 것입니다.

