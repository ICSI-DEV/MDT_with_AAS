# OpenMOS_MDT with AAS 

AAS Server v0.1

# 소개
이 프로젝트는 Eclipse [BaSyx](https://wiki.eclipse.org/BaSyx)기반 Industrie 4.0 AAS(Asset Administration Shell)의 구현입니다.

프로젝트는 아직 진행중이며, 다른 응용 프로그램, 설비(장치, 센서 등), 자산 관리 쉘 등 과의 표준화된 데이터 통신을 목적으로 합니다.
# AAS Server 개요
Server를 실행하기 위해서 HSQLDB가 필요합니다.

<p align="center">
  <img src="https://user-images.githubusercontent.com/75360342/116204043-4ecdf780-a777-11eb-99d7-1c55d4170013.png"/>
  <p align="center" >
    <b>그림 1 AAS Server 구조도</b>
  </p>
</p> 

AAS Server는 응용 프로그램과의 통신에서 데이터 유형에 따라 각각의 구현된 인터페이스를 통해 자산 관리 쉘 모델을 구현합니다.

모델 구현의 경우 라이브러리로 포함 된 BaSyx SDK의 메서드를 통해 구현되며, 모델 구현을 위해 다른 응용 프로그램, 데이터베이스와의 연결이 필요합니다.   

현재 자산 관리 쉘 구현 데이터를 위한 한가지 연결이 있으며, 추후 인터페이스는 확장 및 수정 될 예정입니다. 

# 자산 관리 쉘 모델 구조 

현재 구현 가능한 자산 관리 쉘 모델의 구조는 다음과 같습니다.


--수정중--
