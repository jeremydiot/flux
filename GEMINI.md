# GEMINI.md - FLUX

## 1. Project description
FLUX is an java api based to Netty Project Reactor. It allows to exchange data between two applications in real time. It use HTTP protocol to communicate. It is design for high performance and high scalability.

### 1.1. Features
- Real-time data exchange
- High performance
- High scalability
- From client pull data from a server
- From client push data request to a server
- Pause and resume data exchange
- Push data from a flux to another flux
- Control and monitorize data exchange
- Secure data exchange
- HTTPS (TLS) supported for secure data exchange
- Authentification mechanism
- Pooled connection
- Back pressure

### 1.2. Supported configuration
- Chunk size
- Retry Policy
- Timeout Policy
- Logging level for debug
- Max connection per host
- Max connection per client
- Max connection per server
- Pool size for Flux Stream
- Back pressure size
- Keep alive connection

## 2. Technical stack

All version of the following stack is declared in the pom.xml file and should be used for all the project.

- Langage : Java
- Library : Project Reactor Netty, HttpServer, HttpClient
- Packaging : Maven, Jar (with dependencies)
- Testing : JUnit, hamcrest, Reactor Test, Mockito

### 3 Architecture
Based on standard Maven Layout

```text
flux/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/flux/
│   │   │       ├── client/      # HTTP Client for pulling/pushing
│   │   │       ├── server/      # HTTP Server and request handling
│   │   │       ├── core/        # Core logic, flux pooling, back pressure
│   │   │       ├── config/      # Chunk size, retry, timeout configs
│   │   │       ├── security/    # TLS, Authentication
│   │   │       ├── codec/       # High-performance data serialization & deserialization
│   │   │       └── exception/   # Custom API exceptions
│   │   └── resources/
│   │       └── logback.xml      # Logging configuration
│   └── test/
│       ├── java/
│       │   └── com/flux/        # Unit & Integration tests
│       └── resources/
```

## 4. Useful Commands
* **build :** mvn clean compile
* **package :** mvn package
* **test :** mvn test

## 5. API Usage scenarios
In all following  exemples, there is three applications : 
- APP_CLIENT1 : send request to server
- APP_CLIENT2 : send request to server
- APP_SERVER : send response to client

### 5.1. Pull
1. APP_CLIENT1 asking APP_SERVER to get data flux.
2. APP_SERVER return flux to APP_CLIENT1 with all chunked data.
3. APP_CLIENT1 send acknowledg reception of all data to APP_SERVER.

### 5.2. Push
1. APP_CLIENT1 send chunked data flux to APP_SERVER.
2. APP_SERVER return an acknowledge response for the reception of all chunked data to APP_CLIENT1.

### 5.3. Pause / Resume / Push data from a flux to another flux
1. APP_CLIENT1 asking APP_SERVER to get data flux.
2. APP_SERVER keep open and save the connection with APP_CLIENT1, not respond immediately.
3. APP_CLIENT2 send chunked data flux to APP_SERVER.
4. APP_SERVER respond to APP_CLIENT1 with chunked data from the flux of APP_CLIENT2.
5. APP_CLIENT1 acknowledge the reception of all chunked data to APP_SERVER.
6. APP_SERVER acknowledge the reception of all chunked data to APP_CLIENT2.


