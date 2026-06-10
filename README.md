# Off-Chain-Agent

## Overview

Off-chain agent is a service that runs on Fly.io and is responsible for orchestrating the off-chain operations of the platform. It is responsible for the following:

- Daily backups for disaster recovery
- Monitoring

## Operations

- Self-hosted Airflow migration and operating notes: [docs/self-hosted-airflow.md](docs/self-hosted-airflow.md)

## Architecture

### Periodic Backups

```mermaid
flowchart TD
    IndividualCansiter1[(Individual User <br>Canister 1)]
    IndividualCansiter2[(Individual User <br>Canister 2)]
    UserIndex[(User Index)]
    PlatformOrchestrator[Platform <br>Orchestrator]
    OffChainAgent[OffChain Agent]
    CFWorker[Cloudflare Worker]
    R2[(R2 store)]

    OffChainAgent --[1.1]--> PlatformOrchestrator
    OffChainAgent --[1.2]--> UserIndex
    OffChainAgent --[2(get snapshot)]--> IndividualCansiter1
    OffChainAgent --[2.1(store snapshot)]--> R2
    OffChainAgent --[3]--> IndividualCansiter2
    OffChainAgent --[3.1]--> R2

    CFWorker -- 1(cron trigger) --> OffChainAgent


    subgraph OnChain
        PlatformOrchestrator
        UserIndex
        IndividualCansiter1
        IndividualCansiter2
    end
```

### Video Processing Pipeline (NSFW detection)

```mermaid
flowchart TD
    OffChainAgent[OffChain Agent]
    Frontend[Frontend SSR]
    CFStream[Cloudflare<br> Stream]
    GCSVideos[GCS Videos bucket]
    GCSFrames[GCS Frames bucket]
    NSFWServer[NSFW Server]
    BQEmbedding[BQ Embedding table]
    BQNSFW[BQ NSFW table]
    Upstash[Upstash]

    Frontend --[1]--> CFStream
    Frontend --[2.1]--> OffChainAgent
    OffChainAgent --[2.x.1]--> Upstash
    Upstash --[2.x.2]--> OffChainAgent
    OffChainAgent --[2.2 (from Q1)]--> GCSVideos
    OffChainAgent --[2.3 (from Q2)]--> GCSFrames
    OffChainAgent --[2.4 (from Q3)]--> NSFWServer
    BQEmbedding --[3.1]--> GCSVideos
    NSFWServer --[2.4.1]--> GCSFrames
    OffChainAgent --[2.5]--> BQNSFW

    subgraph GCS
        GCSVideos
        GCSFrames
    end

    subgraph BigQuery
        BQEmbedding
        BQNSFW
    end

```
