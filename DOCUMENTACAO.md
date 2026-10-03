# Documentação da Solução — Freela Marketplace

Plataforma de marketplace de freelancers construída como microsserviços com
comunicação assíncrona baseada em eventos (Apache Kafka), seguindo o estilo de
**coreografia** e o padrão **Transactional Outbox**. Este documento descreve a
arquitetura, os eventos, as estratégias adotadas e o **porquê** de cada decisão.

- **Stack:** Java 25, Spring Boot 4.1, Spring Cloud Gateway, Maven multi-módulo.
- **Mensageria:** Apache Kafka 4.2 em modo KRaft (2 brokers).
- **Persistência:** PostgreSQL 16 (um banco por serviço).
- **Observabilidade:** Graylog (logs centralizados) + Zipkin (tracing distribuído).

---

## 1. Visão Geral da Arquitetura

```
                 ┌──────────────┐
  Cliente HTTP → │  API Gateway │  (gera/propaga X-Correlation-Id)
                 └──────┬───────┘
                        │ lb:// (Eureka)
                 ┌──────▼─────────┐
                 │ contrato-service│  Escreve no banco + Outbox (mesma TX)
                 └──────┬─────────┘
                        │ EventPoller (polling do Outbox) → Kafka
          ┌─────────────┼───────────────────────────┐
          │             │                           │
    criado/entrega/concluido/cancelado (tópicos Kafka, key = contratoId)
          │             │                           │
   ┌──────▼─────┐ ┌─────▼──────┐          ┌─────────▼────────┐
   │notificacao │ │ reputacao  │          │    auditoria     │
   │  -service  │ │  -service  │          │     -service     │
   └──────┬─────┘ └─────┬──────┘          └─────────┬────────┘
          │             │                           │
     notificacao_db  reputacao_db              auditoria_db
```

Todos os serviços enviam **logs para o Graylog** (GELF/UDP) e **traces para o
Zipkin**. O contexto de rastreamento (`traceId`) é propagado também através do
Kafka (headers), conectando o fluxo HTTP → produtor → consumidores.

### Serviços participantes

| Serviço | Porta | Banco | Responsabilidade |
|---|---|---|---|
| `eureka-server` | 8761 | — | Service discovery |
| `api-gateway` | 8080 | — | Ponto de entrada; gera `correlationId` e roteia |
| `contrato-service` | 8081 | `contrato_db` | **Produtor** dos eventos de contrato |
| `notificacao-service` | 8082 | `notificacao_db` | Registra notificações do ciclo de vida |
| `reputacao-service` | 8083 | `reputacao_db` | Atualiza reputação do freelancer |
| `auditoria-service` | 8084 | `auditoria_db` | Registra todos os eventos para consulta |

### Por que coreografia (e não orquestração)?

Cada consumidor reage a eventos de forma **autônoma e desacoplada**, sem um
orquestrador central. O `contrato-service` não conhece seus consumidores, apenas
publica fatos ocorridos no domínio. Isso reduz acoplamento, permite adicionar
novos consumidores sem alterar o produtor e aumenta a resiliência (um consumidor
fora do ar não afeta os demais).

---
## 2. Infraestrutura

Tudo roda via `infra/docker-compose.yml`, preservando os componentes originais e
adicionando os de observabilidade:

- **Eureka Server**, **API Gateway**.
- **Kafka em modo KRaft** (2 brokers: `kafka-1`, `kafka-2`) + **Kafka UI**.
- **PostgreSQL** com bancos separados por serviço (`init-databases.sql`).
- **Zipkin** (tracing) e **Graylog** + MongoDB + OpenSearch (logs).

### Inicialização

```bash
# 1. Build dos artefatos (na raiz do projeto)
mvn clean package -DskipTests

# 2. Subir toda a infraestrutura + serviços
docker compose -f infra/docker-compose.yml up -d --build

# 3. Acompanhar logs (opcional)
docker compose -f infra/docker-compose.yml logs -f contrato-service

# Rebuild apenas de um serviço alterado
docker compose -f infra/docker-compose.yml build contrato-service
docker compose -f infra/docker-compose.yml up -d contrato-service
```

### Endpoints úteis

| Recurso | URL |
|---|---|
| API Gateway | http://localhost:8080 |
| Eureka | http://localhost:8761 |
| Kafka UI | http://localhost:8090 |
| Zipkin | http://localhost:9411 |
| Graylog | http://localhost:9000 (admin / admin) |

---
## 3. Tópicos Kafka

| Tópico | Produtor | Consumidores | Chave (key) |
|---|---|---|---|
| `freela-marketplace.contrato.criado` | contrato-service | notificacao, auditoria | `contratoId` |
| `freela-marketplace.contrato.entrega-registrada` | contrato-service | notificacao, auditoria | `contratoId` |
| `freela-marketplace.contrato.concluido` | contrato-service | notificacao, reputacao, auditoria | `contratoId` |
| `freela-marketplace.contrato.cancelado` | contrato-service | notificacao, auditoria | `contratoId` |

- **Partições:** 3 por tópico. **Replicação:** 2 (um por broker).
- Tópicos criados declarativamente via beans `NewTopic`
  (`contrato-service/.../messaging/KafkaTopicsConfig.java`), garantindo o número
  de partições em vez de depender do auto-create (que criaria 1 partição).

### Tópicos de Dead Letter (DLT)

| Tópico | Origem |
|---|---|
| `notificacao-service.DLT` | falhas no notificacao-service |
| `reputacao-service.DLT` | falhas no reputacao-service |
| `auditoria-service.DLT` | falhas no auditoria-service |

---

## 4. Especificação das Mensagens (Contrato de Comunicação)

Formato: **JSON** no `value`; chave (`key`) é o `contratoId` (String). Metadados
transportados em **headers**: `eventId`, `eventType`, `correlationId` e os headers
de tracing (`traceparent`/`b3`). O `value` sempre inclui `eventId` e `occurredAt`.

Campos obrigatórios em **todos** os eventos: `eventId`, `occurredAt`, `contratoId`.

### 4.1 ContratoCriado

- **Tópico:** `freela-marketplace.contrato.criado`
- **Produtor:** contrato-service · **Consumidores:** notificacao, auditoria
- **Chave:** `contratoId`

| Campo | Tipo | Obrigatório |
|---|---|---|
| `eventId` | UUID (String) | sim |
| `occurredAt` | ISO-8601 (String) | sim |
| `contratoId` | UUID | sim |
| `clienteId` | String | sim |
| `freelancerId` | String | sim |
| `titulo` | String | sim |
| `valor` | String (decimal) | sim |

```json
{
  "eventId": "5c9f1d3e-2b7a-4a6c-9e1f-8d2c4b6a0e11",
  "occurredAt": "2026-10-03T11:22:33.123Z",
  "contratoId": "a1b2c3d4-0000-1111-2222-333344445555",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "22222222-2222-2222-2222-222222222222",
  "titulo": "Landing page institucional",
  "valor": "1500.00"
}
```

### 4.2 EntregaRegistrada

- **Tópico:** `freela-marketplace.contrato.entrega-registrada`
- **Produtor:** contrato-service · **Consumidores:** notificacao, auditoria
- **Chave:** `contratoId`

| Campo | Tipo | Obrigatório |
|---|---|---|
| `eventId` | UUID (String) | sim |
| `occurredAt` | ISO-8601 (String) | sim |
| `contratoId` | UUID | sim |
| `clienteId` | String | sim |
| `freelancerId` | String | sim |

```json
{
  "eventId": "7a1b...",
  "occurredAt": "2026-10-03T11:25:00.000Z",
  "contratoId": "a1b2c3d4-0000-1111-2222-333344445555",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "22222222-2222-2222-2222-222222222222"
}
```

### 4.3 ContratoConcluido

- **Tópico:** `freela-marketplace.contrato.concluido`
- **Produtor:** contrato-service · **Consumidores:** notificacao, **reputacao**, auditoria
- **Chave:** `contratoId`

| Campo | Tipo | Obrigatório |
|---|---|---|
| `eventId` | UUID (String) | sim |
| `occurredAt` | ISO-8601 (String) | sim |
| `contratoId` | UUID | sim |
| `clienteId` | String | sim |
| `freelancerId` | String | sim |
| `valor` | String (decimal) | sim |

```json
{
  "eventId": "9f3c...",
  "occurredAt": "2026-10-03T12:00:00.000Z",
  "contratoId": "a1b2c3d4-0000-1111-2222-333344445555",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "22222222-2222-2222-2222-222222222222",
  "valor": "1500.00"
}
```

### 4.4 ContratoCancelado

- **Tópico:** `freela-marketplace.contrato.cancelado`
- **Produtor:** contrato-service · **Consumidores:** notificacao, auditoria
- **Chave:** `contratoId`

| Campo | Tipo | Obrigatório |
|---|---|---|
| `eventId` | UUID (String) | sim |
| `occurredAt` | ISO-8601 (String) | sim |
| `contratoId` | UUID | sim |
| `clienteId` | String | sim |
| `freelancerId` | String | sim |

```json
{
  "eventId": "1d2e...",
  "occurredAt": "2026-10-03T12:10:00.000Z",
  "contratoId": "a1b2c3d4-0000-1111-2222-333344445555",
  "clienteId": "11111111-1111-1111-1111-111111111111",
  "freelancerId": "22222222-2222-2222-2222-222222222222"
}
```

### Headers (comuns a todos os eventos)

| Header | Descrição |
|---|---|
| `eventId` | Identificador único do evento (idempotência) |
| `eventType` | Nome lógico do evento (ex.: `ContratoCriado`) |
| `correlationId` | Correlação da operação ponta a ponta |
| `traceparent` / `b3` | Contexto de tracing (injetado pelo propagator) |

---

### OBS: Mapeamento Evento de Domínio → Evento Kafka (sem if/else)

Acredito que consergui fazer a forma mais correta para o produtor converte 
cada evento de domínio no evento Kafka correspondente usando 
**polimorfismo + injeção de dependência**, sem cadeias de `if/else` ou `switch`.

- `DomainEventMapper<D, K>`: interface genérica. Cada implementação declara o tipo
  de domínio que trata (`domainEventType()`) e sabe convertê-lo (`toKafkaEvent`).
- `EventMapper`: recebe **a lista de todos os mappers** via construtor (Spring
  injeta todos os beans) e monta um `Map<Class, DomainEventMapper>`. O `toKafkaEvent`
  faz um simples lookup por `event.getClass()`.
- `EventTopics`: mesma ideia — `Map<Class, String>` resolve o tópico pelo tipo do
  evento.

**Por quê:** Ao longo da minha experiência, percebi que quando trabalhamos com eventos de domínio
acabamos criando muitas estruturas de decisão, logo quis usar essa oportunidade para tentar uma 
abordagem mais limpa, extensível e de fácil manutenção. Assim,
adicionar um novo evento passa a exigir apenas uma nova classe de
mapper, sem tocar em código existente usando o princípio
Aberto/Fechado. O compilador e o Spring garantem o registro automático.

```java
// EventMapper — núcleo do mapeamento
public KafkaEvent toKafkaEvent(DomainEvent domainEvent) {
    var mapper = mappers.get(domainEvent.getClass());
    if (mapper == null) throw new IllegalArgumentException(...);
    return mapper.toKafkaEvent(domainEvent);
}
```

---

## 5. Publicação Transacional (Transactional Outbox)

1. Na **mesma transação** que altera o contrato, o `EventPublisher.publish`
   (anotado `@Transactional(propagation = MANDATORY)`) grava o evento serializado
   na tabela `outbox_events`. Se a transação de negócio falhar, o evento também é
   descartado (atomicidade).
2. Um `EventPoller` (`@Scheduled` a cada 1s, `@Transactional`) lê os registros
   ainda não enviados (`sent_at IS NULL`, ordenados por `seq`) e os publica no
   Kafka via `publishOutbox`, marcando `sent_at` após o envio.
3. O produtor Kafka é **idempotente** (`enable.idempotence=true`, `acks=all`),
   evitando duplicatas no broker em caso de retry de rede.

**Por quê:** Assim garantimos a semântica *at-least-once* de ponta a ponta, sem XA/2PC. A
ordenação por `seq` preserva a ordem de criação dos eventos de um mesmo contrato
na publicação. Em caso de falha de publicação, o registro permanece no Outbox e é
retentado no próximo ciclo (log `outbox.publicacao.falhou`).

**Complexidade:** o código do `EventPoller` é simples, mas ao usar ele tive uma dificuldade absurda
ao trabalhar com o mesmo tracer do request original, então precisei salvar o contexto de trace 
no Outbox e restaurá-lo na publicação.

> O `traceContext` é salvo junto para que o span de publicação seja reconstruído
> no momento do envio o poller roda fora do request original, mantendo o trace
> contínuo mesmo com a publicação assíncrona.

**Tabela `outbox_events`:** `id` (eventId), `seq` (bigserial), `aggregateId`
(contratoId), `eventType`, `topic`, `payload`, `correlationId`, `traceContext`,
`createdAt`, `sentAt`.

---

## 6. Particionamento, Concorrência e Ordenação

**Requisito:** processar em paralelo eventos de contratos **distintos**, mas
preservar a ordem dos eventos de um **mesmo** contrato (ex.: `ContratoCriado` →
`EntregaRegistrada` → `ContratoConcluido`).

**Estratégia:**

- **Chave de particionamento = `contratoId`.** O Kafka garante que mensagens com a
  mesma chave vão sempre para a **mesma partição**, e cada partição é consumida em
  ordem por um único consumidor dentro do grupo. Logo, a ordem por contrato é
  preservada.
- **3 partições por tópico** + **`spring.kafka.listener.concurrency: 3`** nos
  consumidores. Isso permite até 3 threads de consumo simultâneas por serviço,
  processando contratos diferentes em paralelo (partições diferentes), sem quebrar
  a ordem dentro de cada contrato.

>Configuração relevante:
> - Produtor: `contrato-service/.../messaging/KafkaTopicsConfig.java` (NewTopic, 3p/2r).
> - Consumidores: `listener.concurrency: 3` em cada `application.yml`.

---

## 7. Idempotência (Tratamento de Duplicatas)

Como a entrega é *at-least-once*, um consumidor pode receber a **mesma mensagem
mais de uma vez**. Cada evento carrega um `eventId` único, usado para deduplicação.

| Serviço | Mecanismo | Local |
|---|---|---|
| notificacao | tabela `processed_events` (`existsById(eventId)`) | `NotificacaoService.registrar` |
| reputacao | tabela `processed_events` (`existsById(eventId)`) | `ReputacaoService.registrarContratoConcluido` |
| auditoria | `repository.existsByEventId(eventId)` | `AuditoriaService.registrar` |

**Padrão:** a verificação de duplicata e a escrita de negócio ocorrem na **mesma
transação** (`@Transactional`). Se o `eventId` já foi processado, o serviço
registra `*.evento.duplicado.ignorado` e retorna sem efeitos colaterais. Assim, o
reprocessamento **não** duplica registros, **não** incrementa reputação indevida e
**não** gera notificações repetidas.

**Por quê guardar `eventId` na mesma TX:** garante atomicidade entre "fazer o
trabalho" e "marcar como processado" não há janela em que um foi feito sem o
outro.

---

## 8. Tratamento de Falhas (Retry + DLT)

Implementado por consumidor (padrão *Dead Letter Topic*), em
`KafkaErrorHandlingConfig` + `FailedEventRecoverer`.

Retry com backoff exponencial `ExponentialBackOff` com intervalo inicial 1s, multiplicador 2, máximo 10s e `maxAttempts=3`.
- **Exceções retryáveis:**  Falhas como banco momentaneamente indisponível são retentadas automaticamente.
- **Exceções não-retryáveis:** `JacksonException` para payload malformado e
  `IllegalArgumentException`como, UUID inválido, vão direto para a DLT, sem
  desperdiçar tentativas. 

Esgotadas as tentativas, o `DeadLetterPublishingRecoverer` publica a mensagem no tópico 
`<serviço>-service.DLT` e persiste o detalhe na tabela `failed_events` 
(payload, headers, stacktrace, offset, etc.) para diagnóstico e reprocessamento. Assim 
a falha de uma mensagem **não** bloqueia nem descarta as demais o offset avança e outros 
contratos/partições seguem normalmente.

>**Reprocessamento:** endpoint `POST /dlt/falhas/{id}/reprocessar` relê o registro
>de `failed_events` e republica no tópico original (headers preservados). Também há 
> `GET /dlt/falhas` e `GET /dlt/falhas/{id}` para consulta.

**OBS:** Pensei em criar um pacote centralizado para DLT mas manter a DLT dentro de cada consumidor 
preserva a autonomia do serviço e o contexto da falha (schema, dependências, regras).

---

## 9. Logs (Conteúdo e Boas Práticas)

Todos os serviços usam logs estruturados em formato `chave=valor`, com um padrão de
nomes `contexto.acao` (ex.: `notificacao.evento.recebido`,
`reputacao.atualizacao.sucesso`). Campos presentes conforme aplicável:

- `service` (via `staticField` do GELF), `eventId`, `eventType`, `contratoId`,
  `correlationId`, `startedAt`/`endedAt` (início/fim), falhas (`*.dlt`,
  `*.retry`), mensagens recebidas (`*.evento.recebido`) e publicadas
  (`outbox.evento.gravado`).

---

## 10. Centralização de Logs (Graylog)

- Cada serviço possui `logback-spring.xml` com um appender **GELF/UDP** apontando
  para `graylog:12201`, além do appender de console.
- `includeMdcData=true` → o `correlationId` (colocado no MDC pelos listeners e pelo
  gateway) é enviado como campo pesquisável.
- `staticField service:${appName}` identifica a origem de cada log.

Permitindo pesquisar no Graylog por `correlationId`, `eventId` ou
`contratoId` e acompanhar uma operação **cruzando vários serviços**, sem abrir o
console de cada aplicação.

---

## 11. Rastreamento Distribuído (Zipkin)

- O contexto de trace é propagado pelo Kafka: o `EventPublisher` utiliza os headers 
do evento para passar informações de contexto, e os consumidores o extraem automaticamente.
- Como a publicação real ocorre no `EventPoller`, o trace
  original é salvo no Outbox `traceContext` e "restaurado" na publicação,
  garantindo um trace contínuo HTTP → Kafka → consumidores.

**Resultado:** no Zipkin é possível visualizar a participação de gateway,
contrato-service e consumidores em uma mesma operação.

---

## 12. Correlação das Operações (correlationId)

- O **API Gateway** lê o header `X-Correlation-Id`; se ausente, **gera um UUID**. 
O valor é repassado adiante no header da requisição.
- O `contrato-service` coloca o `correlationId` no MDC e o grava no Outbox;
  na publicação, vai como header Kafka `correlationId`.
- Os consumidores leem o header, colocam no MDC `MDC.put("correlationId", ...)` e
  limpam ao final `MDC.clear()`, de modo que todos os logs daquela operação
  carregam o mesmo `correlationId`.

**Fluxo correlacionado:** API Gateway → contrato-service → Kafka → consumidores,
tudo pesquisável pelo mesmo `correlationId` no Graylog.

---

## 14. Exemplos de Chamadas (geração de eventos)

Todas as chamadas passam pelo **gateway** (porta 8080). O header
`X-Correlation-Id` é opcional (gerado se ausente).

```bash
# Criar contrato → publica ContratoCriado
curl -s -X POST http://localhost:8080/api/contratos \
  -H "Content-Type: application/json" \
  -H "X-Correlation-Id: demo-001" \
  -d '{
        "clienteId": "11111111-1111-1111-1111-111111111111",
        "freelancerId": "22222222-2222-2222-2222-222222222222",
        "titulo": "Landing page institucional",
        "valor": 1500.00
      }'
# → responde 201 com { "id": "<contratoId>", ... }

# Registrar entrega → publica EntregaRegistrada
curl -s -X POST http://localhost:8080/api/contratos/<contratoId>/entrega \
  -H "X-Correlation-Id: demo-001"

# Concluir → publica ContratoConcluido (dispara reputacao)
curl -s -X POST http://localhost:8080/api/contratos/<contratoId>/concluir \
  -H "X-Correlation-Id: demo-001"

# Cancelar → publica ContratoCancelado
curl -s -X POST http://localhost:8080/api/contratos/<contratoId>/cancelar \
  -H "X-Correlation-Id: demo-001"

# Consultas
curl -s http://localhost:8080/api/contratos
curl -s http://localhost:8080/api/contratos/<contratoId>

# DLT (por serviço) — listar/reprocessar falhas
curl -s http://localhost:8082/dlt/falhas
curl -s -X POST http://localhost:8082/dlt/falhas/<id>/reprocessar
```

---

## 15. Como Verificar os Fluxos (Evidências)

| Evidência | Onde observar |
|---|---|
| Requisição recebida no gateway | log `gateway.request.inicio` (Graylog) |
| Alteração persistida no contrato | `contrato_db` + resposta 201 |
| Evento publicado no Kafka | Kafka UI (tópico) / log `outbox.evento.gravado` |
| Consumo pelos serviços | logs `*.evento.recebido` de cada consumidor |
| Persistência dos consumidores | `notificacao_db`, `reputacao_db`, `auditoria_db` |
| Mensagem duplicada tratada | log `*.evento.duplicado.ignorado` ao reprocessar o mesmo `eventId` |
| Ordem por contrato | eventos do mesmo `contratoId` na mesma partição, em ordem |
| Logs centralizados | pesquisa por `correlationId` no Graylog cruzando serviços |
| Trace no Zipkin | busca pelo trace da operação em http://localhost:9411 |


---
