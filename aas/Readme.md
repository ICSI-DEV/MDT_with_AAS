#AAS Server v0.1
MDT_with_AAS 자산 관리 쉘 서버

#소개
[BaSyx](https://www.eclipse.org/basyx/) SDK를 이용하여 구현한 것으로 BaSyx 메서드에 접근하여 자산 관리 쉘을 구현 합니다.

이 프로젝트는 아직 진행중입니다.

#개요
AAS Server는 타 서비스에 종속되지 아니하며, 독립적으로 운용됩니다. 

구현된 자산 관리 쉘은 서버와 레지스트리에 등록되며, HTTP/REST를 이용하여 데이터를 통신합니다.

HTTP/REST 명령을 통해 호출된 데이터는 VAB의 구현을 통해 각 서비스, 장치에 알맞은 프로토콜로 변환되며 TCP(OPC-UA, MQTT...) 프로토콜 통신이 가능합니다.
