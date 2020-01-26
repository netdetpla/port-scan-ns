FROM openjdk:11.0.5-jre-stretch

ADD ["sources.list", "/etc/apt/"]

RUN apt update && apt install -y nmap

ADD ["build/libs/port-scan-ns-1-all.jar", "settings.properties", "/"]

CMD java -jar port-scan-ns-1-all.jar