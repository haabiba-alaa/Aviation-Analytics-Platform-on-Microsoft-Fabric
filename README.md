```mermaid
flowchart TD
    %% Custom Styling
    classDef batch fill:#E3F2FD,stroke:#1565C0,stroke-width:2px;
    classDef stream fill:#FFF3E0,stroke:#E65100,stroke-width:2px;
    classDef lakehouse fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px;
    classDef sql fill:#F3E5F5,stroke:#6A1B9A,stroke-width:2px;
    classDef bi fill:#FCE4EC,stroke:#C2185B,stroke-width:2px;

    subgraph Batch_Pipeline["Batch Pipeline (Daily at 2:00 AM)"]
        A[Batch Historical Files 5M+ Records] ::: batch --> B[Dataflow Gen2 Ingestion] ::: batch
        B --> C[dbo.bronze_flights] ::: lakehouse
        C --> D[PySpark nb_batch_medallion] ::: batch
        D --> E[dbo.gold_fact_flights & Dimensions] ::: lakehouse
    end

    subgraph Streaming_Pipeline["Streaming Pipeline (Every 15-30 Mins)"]
        F[Live Flight JSON Events] ::: stream --> G[Fabric Eventstream] ::: stream
        G --> H[dbo.bronze_live_telemetry] ::: lakehouse
        H --> I["PySpark Micro-Batch nb_stream_silver (trigger: availableNow)"] ::: stream
        I --> J[dbo.silver_live_telemetry] ::: lakehouse
    end

    subgraph Unified_Analytics["Virtual Layer & Reporting"]
        E --> K["T-SQL View: dbo.gold_vw_all_flights_unified (SQL Analytics Endpoint)"] ::: sql
        J --> K
        K --> L["Power BI Direct Lake Model (sm_aviation_analytics_directlake)"] ::: bi
    end
```
